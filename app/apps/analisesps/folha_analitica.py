# -*- coding: utf-8 -*-
"""
A FOLHA ANALÍTICA DA CONTABILIDADE ("Folha de Pagamento" do Fortes) — 01/10/2026.

Pedido do dono: *"A contabilidade nos envia dois arquivos: um analítico e um
resumido. O que a gente importou é o resumido. (…) Na hora que eu abrisse o
analítico do colaborador, eu visualizar do mês o detalhamento desses valores (…)
entender como é que se chegou àquele valor. Porque às vezes a gente tem uma
dúvida e precisa recorrer à folha."*

O ARQUIVO (lido em 01/10/2026 do de 09/2026, ~7.600 linhas, ~500 pessoas) é um
relatório de impressão, página a página, com um bloco por empregado:

    003048  NOME DO EMPREGADO                       ← código (col A) e nome (col C)
    Cargo: Auxiliar Administrativo                  ← col A
            002 Bolsa-Salário   30 dia(s)   1.234,00          ← evento (col H),
            310 INSS            7,5%                  92,55     referência (col Q),
            …                                                   provento (col T) ou
                                          1.234,00  92,55     desconto (col U)
                         FGTS: 15,23  Líquido a receber: 1.141,45
     Admissão  Dep.  Filhos  Hr/mês  Sal. Cont.  BC-INSS  BC-FGTS
     01/02/26   0      0     080:00   1.234,00   …        …
    Licença por motivo de doença (24/09/2026…)      ← às vezes, uma situação

Entre os blocos: o cabeçalho de cada página ("Folha de Pagamento", "Empresa:",
"Mês/Ano:", "Código | Empregado | Evento…"), a filial ("001 - CONSTRUTORA"), o
setor ("001.01 - CONSTRUTORA/ESCRITORIO") e o TOTAL de cada setor — que repete
a lista de eventos com os totais e PRECISA ser pulado, senão viraria eventos de
ninguém (ou do último empregado lido).

⚠️ O LÍQUIDO DAQUI É O MESMO DA SINTÉTICA, por pessoa — conferido no arquivo de
09/2026 (`conferir_com_a_sintetica`). É isso que permite mostrar a conta no
analítico do funcionário: proventos − descontos = líquido = o que a folha paga.

Este módulo é PURO (lê bytes, devolve estrutura). Guardar e mostrar ficam em
`folha_analitica_guardada` e no analítico (`folha_gestao.ponto_da_pessoa`).
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from decimal import Decimal

from .folha_sintetica import ErroDaFolha, _numero

PADRAO_CODIGO = re.compile(r"^\d{4,6}$")
PADRAO_FILIAL = re.compile(r"^(\d{3}) - (.+)$")
PADRAO_SETOR = re.compile(r"^(\d{3}\.\d{2}) - (.+)$")
PADRAO_EVENTO = re.compile(r"^(\d{3})\s+(.+)$")
PADRAO_MES_ANO = re.compile(r"M[êe]s/Ano:\s*(\d{1,2})/(\d{4})")

# As colunas do relatório (A=0). Fixas no layout do Fortes; se mudarem, a
# conferência com a sintética deixa de bater e a importação recusa.
COL_CODIGO, COL_NOME, COL_EVENTO = 0, 2, 7
COL_REFERENCIA, COL_PROVENTO, COL_DESCONTO = 16, 19, 20
COL_FGTS = 15


@dataclass
class Evento:
    codigo: str
    descricao: str
    referencia: str
    provento: Decimal = Decimal("0.00")
    desconto: Decimal = Decimal("0.00")


@dataclass
class PessoaAnalitica:
    id_fortes: str
    nome: str
    cargo: str = ""
    filial: str = ""
    setor: str = ""
    eventos: list = field(default_factory=list)
    total_proventos: Decimal | None = None
    total_descontos: Decimal | None = None
    liquido: Decimal | None = None
    fgts: Decimal | None = None
    admissao: str = ""
    dependentes: str = ""
    filhos: str = ""
    horas_mes: str = ""
    salario_contribuicao: Decimal | None = None
    base_inss: Decimal | None = None
    base_fgts: Decimal | None = None
    situacao: str = ""      # "Licença por motivo de doença (…)", quando há

    @property
    def soma_proventos(self) -> Decimal:
        return sum((e.provento for e in self.eventos), Decimal("0.00"))

    @property
    def soma_descontos(self) -> Decimal:
        return sum((e.desconto for e in self.eventos), Decimal("0.00"))

    @property
    def fecha(self) -> bool:
        """Os eventos explicam o líquido? (proventos − descontos = líquido)"""
        if self.liquido is None:
            return False
        return self.soma_proventos - self.soma_descontos == self.liquido


@dataclass
class FolhaAnalitica:
    mes: int | None = None
    ano: int | None = None
    pessoas: list = field(default_factory=list)
    avisos: list = field(default_factory=list)

    @property
    def competencia(self) -> str:
        return f"{self.mes:02d}/{self.ano}" if self.mes and self.ano else ""


def _texto(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return " ".join(str(v).split())


def _celula(linha, i):
    return linha[i] if i < len(linha) else ""


def _dinheiro(v) -> Decimal | None:
    n = _numero(v)
    return n.quantize(Decimal("0.01")) if n is not None else None


def _depois_dos_dois_pontos(texto: str) -> str:
    return texto.split(":", 1)[1].strip() if ":" in texto else ""


def _data_do_excel(v) -> str:
    """A admissão vem como número de série do Excel (ou já como texto)."""
    if isinstance(v, (int, float)) and v > 0:
        try:
            base = dt.date(1899, 12, 30)
            return (base + dt.timedelta(days=int(v))).strftime("%d/%m/%Y")
        except (OverflowError, ValueError):
            return ""
    return _texto(v)


def interpretar(linhas) -> FolhaAnalitica:
    """As linhas do relatório → as pessoas com os eventos. FUNÇÃO PURA."""
    folha = FolhaAnalitica()
    filial = setor = ""
    atual: PessoaAnalitica | None = None
    no_total = False            # dentro do bloco "Total: setor …" — pula tudo
    esperando_admissao = False  # a linha seguinte ao cabeçalho "Admissão…"
    depois_da_admissao = False  # o que vier em A agora é a situação da pessoa

    for linha in linhas:
        a = _texto(_celula(linha, COL_CODIGO))
        evento_txt = _texto(_celula(linha, COL_EVENTO))

        m = PADRAO_MES_ANO.search(a)
        if m and folha.mes is None:
            folha.mes, folha.ano = int(m.group(1)), int(m.group(2))
            continue
        if a.startswith(("Folha de Pagamento", "Empresa:", "Mês/Ano", "Emissão:",
                         "Código")) or _texto(_celula(linha, 20)).startswith(
                             ("Continua", "Pag.:")):
            continue

        if esperando_admissao:
            esperando_admissao = False
            if atual is not None:
                atual.admissao = _data_do_excel(_celula(linha, 1))
                atual.dependentes = _texto(_celula(linha, 2))
                atual.filhos = _texto(_celula(linha, 4))
                atual.horas_mes = _texto(_celula(linha, 5))
                atual.salario_contribuicao = _dinheiro(_celula(linha, 7))
                atual.base_inss = _dinheiro(_celula(linha, 10))
                atual.base_fgts = _dinheiro(_celula(linha, 13))
                depois_da_admissao = True
            continue
        if _texto(_celula(linha, 1)) == "Admissão":
            esperando_admissao = True
            continue

        m = PADRAO_SETOR.match(a)
        if m:
            setor, no_total, depois_da_admissao = a, False, False
            continue
        m = PADRAO_FILIAL.match(a)
        if m:
            filial, no_total, depois_da_admissao = a, False, False
            continue
        if a.startswith("Total"):
            no_total, depois_da_admissao = True, False
            continue

        if PADRAO_CODIGO.match(a) and _texto(_celula(linha, COL_NOME)):
            atual = PessoaAnalitica(id_fortes=a.zfill(6),
                                    nome=_texto(_celula(linha, COL_NOME)),
                                    filial=filial, setor=setor)
            folha.pessoas.append(atual)
            no_total = depois_da_admissao = False
            continue
        if no_total or atual is None:
            continue

        if a.startswith("Cargo:"):
            atual.cargo = _depois_dos_dois_pontos(a)
            continue
        if a and depois_da_admissao:
            # Uma linha solta em A depois dos dados de admissão: a situação da
            # pessoa (licença, afastamento…).
            atual.situacao = (atual.situacao + " " + a).strip()
            continue

        m = PADRAO_EVENTO.match(evento_txt)
        if m:
            atual.eventos.append(Evento(
                codigo=m.group(1), descricao=m.group(2),
                referencia=_texto(_celula(linha, COL_REFERENCIA)),
                provento=_dinheiro(_celula(linha, COL_PROVENTO)) or Decimal("0.00"),
                desconto=_dinheiro(_celula(linha, COL_DESCONTO)) or Decimal("0.00")))
            continue

        rotulo_t = _texto(_celula(linha, COL_PROVENTO))
        if rotulo_t.startswith("Líquido"):
            atual.liquido = _dinheiro(_celula(linha, COL_DESCONTO))
            fgts = _texto(_celula(linha, COL_FGTS))
            if fgts.startswith("FGTS"):
                atual.fgts = _dinheiro(_depois_dos_dois_pontos(fgts))
            continue
        prov = _dinheiro(_celula(linha, COL_PROVENTO))
        desc = _dinheiro(_celula(linha, COL_DESCONTO))
        if (prov is not None or desc is not None) and not evento_txt \
                and atual.total_proventos is None:
            atual.total_proventos = prov or Decimal("0.00")
            atual.total_descontos = desc or Decimal("0.00")
            continue

    return folha


def ler(conteudo: bytes) -> FolhaAnalitica:
    """Abre o `.xls` do Fortes e devolve a folha analítica. Levanta ErroDaFolha."""
    if not conteudo:
        raise ErroDaFolha("O arquivo chegou vazio.")
    if len(conteudo) > 30 * 1024 * 1024:
        raise ErroDaFolha("O arquivo tem mais de 30 MB — confira se é a folha "
                          "analítica do Fortes.")
    import xlrd
    try:
        livro = xlrd.open_workbook(file_contents=conteudo)
        aba = livro.sheet_by_index(0)
        linhas = [[aba.cell_value(r, c) for c in range(aba.ncols)]
                  for r in range(aba.nrows)]
    except Exception as e:  # noqa: BLE001
        raise ErroDaFolha(f"Não consegui abrir o arquivo como Excel antigo (.xls): {e}") from e
    folha = interpretar(linhas)
    if not folha.pessoas:
        raise ErroDaFolha("Não achei nenhum empregado neste arquivo. Confira se é a "
                          "Folha de Pagamento (analítica) do Fortes.")
    if folha.mes is None:
        raise ErroDaFolha('Não achei o "Mês/Ano" no arquivo.')
    return folha


def e_analitica(conteudo: bytes) -> bool:
    """O arquivo é a folha ANALÍTICA (e não a sintética)? Olha o cabeçalho."""
    try:
        import xlrd
        aba = xlrd.open_workbook(file_contents=conteudo).sheet_by_index(0)
        for r in range(min(12, aba.nrows)):
            linha = [_texto(aba.cell_value(r, c)) for c in range(aba.ncols)]
            if "Evento" in linha and ("Provento" in linha or "Desconto" in linha):
                return True
    except Exception:  # noqa: BLE001
        return False
    return False


def conferir_com_a_sintetica(analitica: FolhaAnalitica, sintetica) -> dict:
    """O líquido de cada pessoa bate com o da sintética? Devolve o resumo.

    `sintetica`: `folha_sintetica.FolhaLida` (ou as linhas guardadas, com
    `id_fortes` e `valor`)."""
    linhas = getattr(sintetica, "linhas", sintetica) or []
    valor_por_id = {}
    for l in linhas:
        idf = str(getattr(l, "id_fortes", None) or (l.get("id_fortes") if isinstance(l, dict) else "") or "").zfill(6)
        val = getattr(l, "valor", None) if not isinstance(l, dict) else l.get("valor")
        valor_por_id[idf] = Decimal(str(val or 0)).quantize(Decimal("0.01"))
    batem, diferentes, so_na_analitica = 0, [], []
    for p in analitica.pessoas:
        if p.id_fortes not in valor_por_id:
            so_na_analitica.append(p.id_fortes)
            continue
        if (p.liquido or Decimal("0.00")) == valor_por_id[p.id_fortes]:
            batem += 1
        else:
            diferentes.append(p.id_fortes)
    ids = {p.id_fortes for p in analitica.pessoas}
    so_na_sintetica = [i for i in valor_por_id if i not in ids]
    return {"batem": batem, "diferentes": diferentes,
            "so_na_analitica": so_na_analitica, "so_na_sintetica": so_na_sintetica,
            "eventos_explicam": sum(1 for p in analitica.pessoas if p.fecha),
            "pessoas": len(analitica.pessoas)}
