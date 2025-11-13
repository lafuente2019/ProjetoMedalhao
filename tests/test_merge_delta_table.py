from Utils.merge_utils import get_merge_condition


def test_get_merge_condition_bronze_ipca():
    """
    Para as tabelas de bronze ipca/boi_gordo, a condição de merge
    DEVE usar (data, valor).
    """
    cond = get_merge_condition("workspace.bronze_etl.ipca")
    assert "t.data = s.data" in cond
    assert "t.valor = s.valor" in cond
    assert "ipca" not in cond
    assert "boi_gordo" not in cond


def test_get_merge_condition_bronze_boi_gordo():
    """
    Mesmo comportamento para a tabela de boi_gordo.
    """
    cond = get_merge_condition("workspace.bronze_etl.boi_gordo")
    assert "t.data = s.data" in cond
    assert "t.valor = s.valor" in cond


def test_get_merge_condition_outras_tabelas():
    """
    Para qualquer outra tabela (ex: silver, gold),
    a condição DEVE usar (data, ipca, boi_gordo).
    """
    cond = get_merge_condition("workspace.gold_etl.insights")
    assert "t.data = s.data" in cond
    assert "t.ipca = s.ipca" in cond
    assert "t.boi_gordo = s.boi_gordo" in cond
    assert "valor" not in cond
