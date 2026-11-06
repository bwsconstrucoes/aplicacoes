import datetime as dt
import pytest
from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, mundo  # noqa
from tests.test_ponto_tipos_ausencia_banco import _dia
pytestmark = pytest.mark.banco

def test_caso(app, mundo):
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    qui, sex = seg + dt.timedelta(days=3), seg + dt.timedelta(days=4)
    for h, m in ((7, 0), (11, 0), (12, 0), (20, 17)):
        bater_via_chave(app, CPF_JOAO, local(qui, h, m))
    for h, m in ((7, 0), (11, 0), (12, 0), (12, 30)):
        bater_via_chave(app, CPF_JOAO, local(sex, h, m))
    def mostra(rot):
        for d in (qui, sex):
            x = _dia(dp, mundo["joao"], d)
            print(rot, d, x["situacao"], "prev", x.get("previsto"), "trab", x.get("trabalhado"), "extra", x["extra"], "deb", x["debito"], x["alertas"], [y.get("rotulo") for y in x.get("declaracoes") or []], x.get("compensacao"), x.get("ocorrencia"))
    mostra("SEM")
    r = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "COMPENSACAO", "subtipo": "HORAS", "colaborador_id": mundo["joao"],
        "data_inicio": sex.isoformat(), "dia_trabalhado": qui.isoformat(), "aprovar_ja": True})
    print(r.status_code, r.get_json().get("erro"), r.get_json().get("ocorrencia", {}).get("rotulo"))
    mostra("COM")
