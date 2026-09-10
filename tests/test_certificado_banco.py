"""O certificado digital A1 da empresa — com banco de verdade.

Ele fechava dois buracos abertos por trabalho anterior: a emissão automática
da nota (no padrão nacional a DPS vai ASSINADA) e o quarto aviso da agenda,
que ficou de fora porque o certificado não tinha onde morar.

Hoje o .pfx vive no computador de alguém, com a senha num papel. Aí ele vence
num sábado, ninguém sabe, e a obra para de faturar na segunda.

O que se prova:

  1. O arquivo e a senha vão CIFRADOS — quem lê o banco não tem o certificado.
  2. Sem a chave de segredos, o sistema RECUSA guardar. Guardar em claro "só
     desta vez" é como uma assinatura de empresa vaza sem ninguém perceber.
  3. A validade é LIDA de dentro do arquivo, nunca digitada — e abrir o
     arquivo é o que prova que a senha está certa.
  4. Certificado de outro CNPJ é recusado: trocar os arquivos de duas empresas
     faria a nota sair assinada pelo CNPJ errado.
  5. O anterior não é apagado — vira histórico, porque a nota de março foi
     assinada com AQUELE certificado.
  6. O vencimento vira aviso na agenda.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from app.apps.erp.core.agenda import geradores
from app.apps.erp.core.agenda import service as svc_agenda
from app.apps.erp.core.cadastros import certificado as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Empresa, EmpresaCertificado

pytestmark = pytest.mark.banco

CHAVE_TESTE = "wsHQ6Rl6nJcU8mQVzD3s7pQ1nR2tYv0aBcDeFgHiJkL="   # Fernet de teste
SENHA = "senha-do-certificado"


def _fabricar_pfx(*, cnpj: str = "11222333000181", titular: str = "BWS CONSTRUCOES LTDA",
                  dias_de_validade: int = 200, senha: str = SENHA) -> bytes:
    """Um certificado A1 de mentira, mas de verdade: mesma estrutura do real.

    Fabricar aqui é melhor que guardar um .pfx no repositório — certificado
    versionado é certificado vazado, mesmo sendo de teste.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    chave = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    nome = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, f"{titular}:{cnpj}"),
        x509.NameAttribute(NameOID.COUNTRY_NAME, "BR"),
    ])
    emissor = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "AC Teste RFB"),
        x509.NameAttribute(NameOID.COUNTRY_NAME, "BR"),
    ])
    agora = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(nome).issuer_name(emissor)
            .public_key(chave.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(agora - timedelta(days=30))
            .not_valid_after(agora + timedelta(days=dias_de_validade))
            .sign(chave, hashes.SHA256()))
    return pkcs12.serialize_key_and_certificates(
        name=b"teste", key=chave, cert=cert, cas=None,
        encryption_algorithm=serialization.BestAvailableEncryption(senha.encode()))


@pytest.fixture
def cenario(sessao_real, monkeypatch):
    monkeypatch.setenv("ERP_CHAVE_SEGREDOS", CHAVE_TESTE)
    s = sessao_real
    bws = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181")
    outra = Empresa(razao_social="Segunda Construtora LTDA", nome_fantasia="Segunda",
                    cnpj="44555666000199")
    s.add_all([bws, outra])
    s.flush()
    return {"s": s, "bws": bws, "outra": outra, "monkeypatch": monkeypatch}


# ---------------------------------------------------------------------------
# 1 e 3. Guardar, cifrado, com a validade lida do arquivo
# ---------------------------------------------------------------------------
def test_a_validade_vem_de_dentro_do_arquivo(cenario):
    """Campo de data que a pessoa preenche é campo que ela erra — e aqui o
    erro só apareceria no dia em que a nota não sai."""
    s = cenario["s"]
    c = svc.guardar(s, cenario["bws"].id, _fabricar_pfx(dias_de_validade=200), SENHA,
                    nome_arquivo="bws.pfx")
    assert c.valido_ate == (date.today() + timedelta(days=200))
    assert c.titular == "BWS CONSTRUCOES LTDA"
    assert c.documento == "11222333000181"
    assert c.emissor == "AC Teste RFB"
    assert c.numero_serie


def test_o_arquivo_e_a_senha_ficam_cifrados(cenario):
    """Quem tiver uma cópia do banco não tem o certificado — e certificado
    digital é a assinatura da empresa."""
    s = cenario["s"]
    bruto = _fabricar_pfx()
    c = svc.guardar(s, cenario["bws"].id, bruto, SENHA)

    assert SENHA not in c.senha_cifrada
    assert c.arquivo_cifrado != bruto.hex()
    assert len(c.arquivo_cifrado) > 100

    # e volta inteiro para quem vai assinar
    conteudo, senha = svc.material_para_assinar(s, cenario["bws"].id)
    assert conteudo == bruto
    assert senha == SENHA


def test_a_senha_errada_e_recusada_com_frase_util(cenario):
    """Abrir o arquivo é o que PROVA que a senha está certa. Aceitar sem
    conferir deixaria o erro para o dia da emissão."""
    with pytest.raises(ErroValidacao) as e:
        svc.guardar(cenario["s"], cenario["bws"].id, _fabricar_pfx(), "senha-errada")
    assert "Quase sempre é a senha" in str(e.value)


def test_arquivo_que_nao_e_certificado_e_recusado(cenario):
    with pytest.raises(ErroValidacao):
        svc.guardar(cenario["s"], cenario["bws"].id, b"isto nao e um pfx", SENHA)


def test_certificado_vencido_nao_entra(cenario):
    """Guardá-lo criaria a impressão de que a empresa está em dia."""
    s = cenario["s"]
    with pytest.raises(ErroValidacao) as e:
        svc.guardar(s, cenario["bws"].id, _fabricar_pfx(dias_de_validade=-5), SENHA)
    assert "venceu em" in str(e.value)


def test_arquivo_grande_demais_e_recusado(cenario):
    with pytest.raises(ErroValidacao) as e:
        svc.guardar(cenario["s"], cenario["bws"].id, b"x" * (600 * 1024), SENHA)
    assert "alguns kilobytes" in str(e.value)


# ---------------------------------------------------------------------------
# 2. Sem a chave, não se grava
# ---------------------------------------------------------------------------
def test_sem_a_chave_de_segredos_o_sistema_recusa(cenario):
    """A alternativa — guardar em claro 'só desta vez' — é como uma assinatura
    de empresa vaza sem ninguém perceber."""
    cenario["monkeypatch"].delenv("ERP_CHAVE_SEGREDOS", raising=False)
    with pytest.raises(ErroValidacao) as e:
        svc.guardar(cenario["s"], cenario["bws"].id, _fabricar_pfx(), SENHA)
    assert "ERP_CHAVE_SEGREDOS" in str(e.value)
    assert cenario["s"].query(EmpresaCertificado).count() == 0


# ---------------------------------------------------------------------------
# 4. O CNPJ tem de bater
# ---------------------------------------------------------------------------
def test_certificado_de_outro_cnpj_e_recusado(cenario):
    """Trocar os arquivos de duas empresas é o erro fácil de cometer e difícil
    de descobrir: as notas sairiam assinadas pelo CNPJ errado."""
    s = cenario["s"]
    with pytest.raises(ErroValidacao) as e:
        svc.guardar(s, cenario["bws"].id,
                    _fabricar_pfx(cnpj="44555666000199", titular="SEGUNDA LTDA"), SENHA)
    assert "trocou os arquivos" in str(e.value)


# ---------------------------------------------------------------------------
# 5. O anterior vira histórico
# ---------------------------------------------------------------------------
def test_o_certificado_anterior_nao_some(cenario):
    """A nota assinada em março foi assinada com AQUELE certificado, e um dia
    alguém vai perguntar com qual."""
    s = cenario["s"]
    svc.guardar(s, cenario["bws"].id, _fabricar_pfx(dias_de_validade=10), SENHA)
    novo_bruto = _fabricar_pfx(dias_de_validade=400)
    svc.guardar(s, cenario["bws"].id, novo_bruto, SENHA)

    ativos = s.query(EmpresaCertificado).filter_by(
        empresa_id=cenario["bws"].id, situacao="ATIVO").all()
    assert len(ativos) == 1, "um ativo por empresa"
    assert svc.ler(s, cenario["bws"].id)["dias"] == 400

    hist = svc.historico(s, cenario["bws"].id)
    assert len(hist) == 2
    assert {h["situacao"] for h in hist} == {"ATIVO", "SUBSTITUIDO"}

    conteudo, _ = svc.material_para_assinar(s, cenario["bws"].id)
    assert conteudo == novo_bruto, "quem assina usa o novo"


# ---------------------------------------------------------------------------
# 6. A leitura e o aviso
# ---------------------------------------------------------------------------
def test_a_tela_nunca_recebe_o_arquivo_nem_a_senha(cenario):
    """O que não tem porta não é arrombado."""
    s = cenario["s"]
    svc.guardar(s, cenario["bws"].id, _fabricar_pfx(), SENHA)
    lido = svc.ler(s, cenario["bws"].id)
    texto = str(lido)
    assert "arquivo_cifrado" not in lido and "senha" not in texto
    assert lido["titular"] == "BWS CONSTRUCOES LTDA"
    assert lido["tem"] is True


def test_empresa_sem_certificado_diz_que_nao_tem(cenario):
    lido = svc.ler(cenario["s"], cenario["outra"].id)
    assert lido["tem"] is False
    assert lido["ha_chave"] is True


def test_assinar_sem_certificado_da_erro_claro(cenario):
    with pytest.raises(ErroValidacao) as e:
        svc.material_para_assinar(cenario["s"], cenario["outra"].id)
    assert "não tem certificado digital" in str(e.value)


def test_o_vencimento_vira_aviso_na_agenda(cenario):
    """Era o quarto aviso que a agenda prometia e não tinha de onde tirar."""
    s = cenario["s"]
    svc.guardar(s, cenario["bws"].id, _fabricar_pfx(dias_de_validade=20), SENHA)

    eventos = geradores.certificados(s, date.today())
    assert len(eventos) == 1
    assert eventos[0]["origem"] == "CERTIFICADO"
    assert "Vence" in eventos[0]["titulo"]
    assert "contadora" in eventos[0]["detalhe"]

    svc_agenda.sincronizar(s, hoje=date.today())
    na_agenda = svc_agenda.listar(s, hoje=date.today())["eventos"]
    assert any(e["origem"] == "CERTIFICADO" for e in na_agenda)


def test_certificado_longe_de_vencer_nao_avisa(cenario):
    s = cenario["s"]
    svc.guardar(s, cenario["bws"].id, _fabricar_pfx(dias_de_validade=300), SENHA)
    assert geradores.certificados(s, date.today()) == []


def test_o_certificado_substituido_nao_avisa(cenario):
    """Trocar o certificado tem de FECHAR o aviso, não deixar os dois."""
    s = cenario["s"]
    svc.guardar(s, cenario["bws"].id, _fabricar_pfx(dias_de_validade=10), SENHA)
    svc_agenda.sincronizar(s, hoje=date.today())
    assert len([e for e in svc_agenda.listar(s, hoje=date.today())["eventos"]
                if e["origem"] == "CERTIFICADO"]) == 1

    svc.guardar(s, cenario["bws"].id, _fabricar_pfx(dias_de_validade=400), SENHA)
    svc_agenda.sincronizar(s, hoje=date.today())
    assert [e for e in svc_agenda.listar(s, hoje=date.today())["eventos"]
            if e["origem"] == "CERTIFICADO"] == []
