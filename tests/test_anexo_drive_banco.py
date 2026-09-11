"""O anexo morando no Google Drive — com banco de verdade.

Decisão do dono em 08/09/2026: o plano de banco é de 2 GB e a empresa já paga
2 TB de Drive. Anexo é o que mais cresce dentro do banco.

Com banco porque as garantias que interessam são do BANCO: as duas restrições
da migração 043 dizem que anexo no Drive tem de ter o identificador do arquivo,
e anexo no banco tem de ter os bytes. Sem isso um defeito produziria anexo que
não está em lugar nenhum — e ninguém descobriria antes de precisar do documento.

O Drive em si é dublado: ele é serviço de terceiro, não roda aqui, e o que
precisa ser provado é a REGRA, não o Google.

O que se prova:

  1. Com o Drive desligado, nada muda: o anexo continua no banco.
  2. Ligado, o anexo novo vai para o Drive e o banco não guarda os bytes.
  3. Ler de volta funciona nos dois casos — é por um caminho só.
  4. Se o Drive falhar na hora de salvar, o anexo é guardado NO BANCO em vez de
     se perder. Falhar guardando é melhor que falhar perdendo.
  5. Mover os antigos confere a cópia antes de apagar do banco, e uma cópia
     que não confere NÃO apaga nada.
  6. Apagar o anexo apaga o arquivo no Drive junto.
"""
from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.documentos import armazenamento, drive
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, Parametro, Usuario
from app.apps.erp.db.models.financeiro import Anexo

pytestmark = pytest.mark.banco

ARQUIVO = b"%PDF-1.4 comprovante de teste do drive"


class DriveFalso:
    """Um Drive de mentira que guarda em memória e sabe quebrar quando pedido."""

    def __init__(self):
        self.arquivos: dict[str, bytes] = {}
        self.quebra_no_envio = False
        self.corrompe_na_volta = False
        self.apagados: list[str] = []
        self.seq = 0

    def enviar(self, conteudo, nome, mime, *, pasta, impersonar=""):
        if self.quebra_no_envio:
            raise drive.ErroDrive("Drive fora do ar (de mentira)")
        self.seq += 1
        fid = f"fake-{self.seq}"
        self.arquivos[fid] = bytes(conteudo)
        return fid

    def baixar(self, file_id, *, impersonar=""):
        if file_id not in self.arquivos:
            raise drive.ErroDrive("arquivo não existe lá")
        return b"outra coisa" if self.corrompe_na_volta else self.arquivos[file_id]

    def apagar(self, file_id, *, impersonar=""):
        self.apagados.append(file_id)
        self.arquivos.pop(file_id, None)


@pytest.fixture
def cenario(sessao_real, monkeypatch):
    s = sessao_real
    u = Usuario(nome="Admin do anexo", email="anexo.drive@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    s.add(u)
    s.flush()

    falso = DriveFalso()
    monkeypatch.setattr(drive, "enviar", falso.enviar)
    monkeypatch.setattr(drive, "baixar", falso.baixar)
    monkeypatch.setattr(drive, "apagar", falso.apagar)
    return {"s": s, "usuario": u, "drive": falso}


def _ligar(s, ligado=True, pasta="pasta-de-teste"):
    for chave, valor in ((drive.CHAVE_PASTA, pasta),
                         (drive.CHAVE_LIGADO, "1" if ligado else "0"),
                         (drive.CHAVE_IMPERSONAR, "")):
        linha = s.get(Parametro, chave)
        if linha is None:
            linha = Parametro(chave=chave, valor="")
            s.add(linha)
        linha.valor = valor
    s.flush()


def _salvar(cenario, conteudo=ARQUIVO, nome="comprovante.pdf", entidade_id=1):
    return armazenamento.salvar(cenario["s"], conteudo, nome,
                                entidade_tipo="titulo", entidade_id=entidade_id,
                                categoria="COMPROVANTE", usuario=cenario["usuario"])


# ---------------------------------------------------------------------------
# 1 e 2 — desligado não muda nada; ligado tira os bytes do banco
# ---------------------------------------------------------------------------
def test_com_o_drive_desligado_o_anexo_continua_no_banco(cenario, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_BASE64", "x")
    _ligar(cenario["s"], ligado=False)
    a = _salvar(cenario)
    assert a.guardado_em == "BANCO"
    assert a.drive_file_id is None
    assert bytes(a.conteudo) == ARQUIVO
    assert cenario["drive"].arquivos == {}, "não era para ter tocado no Drive"


def test_ligado_o_anexo_novo_vai_para_o_drive(cenario, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_BASE64", "x")
    _ligar(cenario["s"])
    a = _salvar(cenario)
    assert a.guardado_em == "DRIVE"
    assert a.drive_file_id in cenario["drive"].arquivos
    assert a.conteudo is None, "os bytes não podem continuar ocupando o banco"


def test_ler_de_volta_funciona_nos_dois_casos(cenario, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_BASE64", "x")
    s = cenario["s"]
    _ligar(s, ligado=False)
    no_banco = _salvar(cenario, entidade_id=1)
    _ligar(s, ligado=True)
    no_drive = _salvar(cenario, conteudo=ARQUIVO + b" 2", entidade_id=2)
    assert armazenamento.conteudo_de(s, no_banco) == ARQUIVO
    assert armazenamento.conteudo_de(s, no_drive) == ARQUIVO + b" 2"


# ---------------------------------------------------------------------------
# 4 — falhar guardando é melhor que falhar perdendo
# ---------------------------------------------------------------------------
def test_drive_fora_do_ar_guarda_no_banco_em_vez_de_perder(cenario, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_BASE64", "x")
    _ligar(cenario["s"])
    cenario["drive"].quebra_no_envio = True
    a = _salvar(cenario)
    assert a.guardado_em == "BANCO"
    assert bytes(a.conteudo) == ARQUIVO, "o comprovante não pode se perder"


# ---------------------------------------------------------------------------
# 5 — mover os antigos: confere antes de apagar
# ---------------------------------------------------------------------------
def test_mover_leva_para_o_drive_e_esvazia_o_banco(cenario, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_BASE64", "x")
    s = cenario["s"]
    _ligar(s, ligado=False)
    a = _salvar(cenario)
    assert a.guardado_em == "BANCO"

    _ligar(s, ligado=True)
    r = armazenamento.mover_para_drive(s, limite=10)
    assert r["movidos"] >= 1
    s.refresh(a)
    assert a.guardado_em == "DRIVE"
    assert a.conteudo is None
    assert armazenamento.conteudo_de(s, a) == ARQUIVO


def test_copia_que_nao_confere_nao_apaga_nada(cenario, monkeypatch):
    """A regra que não se negocia: nada sai do banco sem cópia conferida."""
    monkeypatch.setenv("GOOGLE_CREDENTIALS_BASE64", "x")
    s = cenario["s"]
    _ligar(s, ligado=False)
    a = _salvar(cenario)

    _ligar(s, ligado=True)
    cenario["drive"].corrompe_na_volta = True
    r = armazenamento.mover_para_drive(s, limite=10)
    assert r["movidos"] == 0
    assert r["falhas"] and "não confere" in r["falhas"][0]["erro"]
    s.refresh(a)
    assert a.guardado_em == "BANCO"
    assert bytes(a.conteudo) == ARQUIVO, "o documento tem de continuar existindo"
    assert cenario["drive"].apagados, "a cópia ruim tinha de ser removida do Drive"


# ---------------------------------------------------------------------------
# 6 — apagar apaga dos dois lados
# ---------------------------------------------------------------------------
def test_excluir_apaga_o_arquivo_no_drive_junto(cenario, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_BASE64", "x")
    s = cenario["s"]
    _ligar(s)
    a = _salvar(cenario)
    fid = a.drive_file_id
    armazenamento.excluir(s, a.id, cenario["usuario"])
    s.flush()
    assert fid in cenario["drive"].apagados, "arquivo órfão no Drive é lixo pago"


# ---------------------------------------------------------------------------
# A garantia do BANCO — a que nenhum código pode furar
# ---------------------------------------------------------------------------
def test_o_banco_recusa_anexo_que_nao_esta_em_lugar_nenhum(cenario):
    s = cenario["s"]
    s.add(Anexo(entidade_tipo="titulo", entidade_id=1, nome_arquivo="x.pdf",
                hash_sha256="a" * 64, guardado_em="BANCO", conteudo=None))
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


def test_o_banco_recusa_drive_sem_identificador(cenario):
    s = cenario["s"]
    s.add(Anexo(entidade_tipo="titulo", entidade_id=1, nome_arquivo="x.pdf",
                hash_sha256="b" * 64, guardado_em="DRIVE", drive_file_id=None,
                conteudo=b"seja o que for"))
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


# ---------------------------------------------------------------------------
# Detalhe pequeno que evita erro de digitação do dono
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("colado,esperado", [
    ("1AbC-dEf", "1AbC-dEf"),
    ("https://drive.google.com/drive/folders/1AbC-dEf", "1AbC-dEf"),
    ("https://drive.google.com/drive/u/0/folders/1AbC-dEf?usp=sharing", "1AbC-dEf"),
    ("  ", ""),
])
def test_aceita_o_endereco_colado_da_barra(colado, esperado):
    assert drive.id_da_pasta(colado) == esperado
