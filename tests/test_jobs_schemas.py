"""Spécifications d'ingestion : schémas, DDL et recensement de la zone d'atterrissage.

Ces tests ne dépendent pas de PySpark et s'exécutent donc sans Spark installé.
La logique qui nécessite un moteur — lecture, validation, MERGE — est vérifiée
de bout en bout par le test de fumée, qui exécute le job réel dans son conteneur
contre de vraies données. Un test unitaire de MERGE avec une session Spark
locale vérifierait surtout que Spark fonctionne.
"""

from __future__ import annotations

import pytest

from common import domain as dom
from jobs.batch import landing
from jobs.batch import schemas as sch

ANNEXE = {
    "bank_txn": {"transaction_id", "timestamp", "account_id", "beneficiary_account",
                 "branch_id", "country_code", "transaction_type", "amount", "currency",
                 "channel", "transaction_status", "fee_amount", "entity_type"},
    "insurance_ops": {"operation_id", "timestamp", "customer_id", "account_id",
                      "country_code", "operation_type", "product_line", "amount",
                      "currency", "claim_status", "processing_days", "entity_type"},
    "mobile_money": {"payment_id", "timestamp", "sender_id", "receiver_id",
                     "sender_country", "receiver_country", "amount", "currency",
                     "payment_type", "operator", "status", "fee_amount", "entity_type"},
    "loan_repayments": {"repayment_id", "timestamp", "loan_account_id", "customer_id",
                        "country_code", "amount_due", "amount_paid", "currency",
                        "due_date", "payment_date", "days_overdue", "loan_type",
                        "repayment_status", "entity_type"},
    "customers": {"customer_id", "country_code", "entity_type", "segment",
                  "kyc_level", "onboarding_date", "region", "is_active"},
    "accounts": {"account_id", "customer_id", "country_code", "account_type",
                 "currency", "balance", "credit_limit", "opened_date", "status"},
    "branches": {"branch_id", "country_code", "entity_type", "city", "region",
                 "branch_type", "is_active"},
}


def test_les_huit_jeux_de_donnees_de_l_enonce_sont_couverts():
    assert len(sch.SPECS) == 8
    assert {spec.table for spec in sch.SPECS} == set(dom.RAW_TABLES)


def test_les_referentiels_sont_traites_avant_les_transactions():
    """L'annexe A.8 l'impose : les clés doivent exister avant d'être référencées."""
    ordre = [spec.is_referential for spec in sch.SPECS]
    assert ordre == sorted(ordre, reverse=True)


@pytest.mark.parametrize("name", sorted(ANNEXE))
def test_schema_couvre_l_annexe(name):
    declarees = {column.name for column in sch.BY_NAME[name].columns}
    assert ANNEXE[name] <= declarees


def test_toutes_les_tables_portent_country_code_et_entity_type():
    """Contrainte transverse de l'énoncé."""
    for spec in sch.SPECS:
        noms = {column.name for column in spec.columns}
        assert "country_code" in noms, spec.name
        assert "entity_type" in noms, spec.name


def test_colonnes_de_tracabilite_ajoutees_partout():
    for spec in sch.SPECS:
        noms = {column.name for column in spec.all_columns}
        assert {sch.INGESTION_TIMESTAMP, sch.SOURCE_FILE} <= noms


@pytest.mark.parametrize("spec", sch.SPECS, ids=lambda s: s.name)
def test_cle_naturelle_declaree_et_obligatoire(spec):
    par_nom = {column.name: column for column in spec.columns}
    assert spec.key in par_nom
    assert par_nom[spec.key].required, "la clé du MERGE ne peut pas être nulle"


@pytest.mark.parametrize("spec", sch.SPECS, ids=lambda s: s.name)
def test_cle_naturelle_conforme_au_domaine(spec):
    connues = {**dom.DATASETS, **dom.REFERENTIALS}
    assert spec.key == connues[spec.name]["key"]
    assert spec.table == connues[spec.name]["table"]


@pytest.mark.parametrize("spec", sch.SPECS, ids=lambda s: s.name)
def test_partitionnement_par_pays_et_par_date(spec):
    """Exigence du §1.3, y compris pour les référentiels, partitionnés sur leur
    date d'ingestion faute d'horodatage métier."""
    assert spec.partitioning.startswith("country_code, days(")
    assert spec.event_time in {column.name for column in spec.all_columns}


@pytest.mark.parametrize("spec", sch.SPECS, ids=lambda s: s.name)
def test_ddl_bien_formee(spec):
    ddl = sch.create_table_ddl(spec, "iceberg.raw." + spec.table)
    assert "CREATE TABLE IF NOT EXISTS iceberg.raw." + spec.table in ddl
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (country_code, days(" in ddl
    # MERGE INTO s'appuie sur les suppressions au niveau ligne, propres au
    # format v2 : une table v1 ferait échouer l'idempotence.
    assert "'format-version' = '2'" in ddl
    for column in spec.all_columns:
        assert "{} {}".format(column.name, column.type) in ddl


@pytest.mark.parametrize("spec", sch.SPECS, ids=lambda s: s.name)
def test_nomenclatures_referencees_existent(spec):
    for name in spec.enums:
        assert name in {column.name for column in spec.columns}
    for name in spec.non_negative:
        assert name in {column.name for column in spec.columns}


def test_seuls_les_referentiels_utilisent_la_date_d_ingestion():
    referentiels = {spec.name for spec in sch.SPECS if spec.is_referential}
    assert referentiels == set(dom.REFERENTIALS)


# --- Recensement de la zone d'atterrissage -----------------------------------


@pytest.mark.parametrize(
    ("key", "attendu"),
    [
        ("referentials/customers.csv", "customers"),
        ("referentials/accounts.csv", "accounts"),
        ("CI/bank_txn/bank_txn_CI_20260331_01.csv", "bank_txn"),
        ("GH/mobile_money/mobile_money_GH_20260331_02.csv", "mobile_money"),
        ("fichier_a_la_racine.csv", ""),
    ],
)
def test_deduction_du_jeu_de_donnees_depuis_le_chemin(key, attendu):
    """Deux conventions coexistent : référentiels partagés à la racine,
    transactions rangées en sous-dossiers pays / type."""
    assert landing._dataset_of(key) == attendu


def test_construction_des_uri_spark():
    uris = landing.to_uris("raw-landing", ["CI/bank_txn/f.csv"])
    assert uris == ["s3a://raw-landing/CI/bank_txn/f.csv"]
