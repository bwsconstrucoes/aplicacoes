"""O FORMATO DOS CAMPOS, VARRIDO EM TODAS AS TELAS — 22/09/2026.

O dono, depois de usar o sistema por algumas horas:

  *"Acho que precisa dar uma reanalisada nos campos. Às vezes tem campo assim
  que fica comendo as informações, deveria ter uma quebra de linha, não tem
  (…) campo de CPF, CNPJ, como não aparecer a formatação, já deveria aparecer.
  Os campos que têm busca, dá uma melhorada nessa engrenagem das buscas (…)
  mas não só para o que eu citei, de uma maneira geral também. Acho que tem que
  fazer uma varredura mais profunda aí."*

**A DECISÃO QUE ESTES TESTES DEFENDEM é onde o conserto mora.** A queixa dele
foi *"todas as telas têm algum detalhe assim"* — e consertar tela por tela
deixaria de fora a próxima que alguém escrever, que é exatamente como o
problema nasceu. Então a máscara, a formatação e a busca vivem em UM lugar
(`erp_base.html`), e são aplicadas SOZINHAS pelo observador que já existia.

Por isso o teste aqui é de REGRA, não de tela: se a regra estiver certa, as 32
telas estão certas, e a 33ª também nasce certa.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

BASE = Path(__file__).resolve().parents[1] / "app/apps/erp/templates/erp_base.html"
TELAS = (Path(__file__).resolve().parents[1] / "app/apps/erp/templates")
CSS = Path(__file__).resolve().parents[1] / "app/apps/erp/static/erp.css"


@pytest.fixture(scope="module")
def base():
    return BASE.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# A máscara existe, é automática, e reconhece o campo pelo NOME
# ---------------------------------------------------------------------------
def test_as_mascaras_sao_aplicadas_sozinhas(base):
    """Se alguém tirar isto, a formatação volta a depender de cada tela lembrar
    — que é como o problema apareceu."""
    assert "function aplicarMascaras" in base
    assert "aplicarMascaras(no.parentNode" in base, \
        "tem de pegar carona no observador: quase todo campo de CNPJ nasce em diálogo"
    assert "aplicarMascaras();" in base, "e rodar também no carregamento da tela"


def test_o_dialogo_perguntar_leva_o_NOME_do_campo(base):
    """São dezenas de diálogos montados por `perguntar()` no ERP. Sem o `name`,
    a máscara não reconhece o campo e todos ficam de fora."""
    assert 'name="${c.nome || ""}"' in base


def test_a_fronteira_do_reconhecedor_aceita_ESPACO(base):
    """O defeito que quase passou: o texto examinado é "id nome" junto, e a
    primeira versão só aceitava hífen e sublinhado como fronteira. O CNPJ
    pegava (por causa do "_" em "cnpj_cpf") e o telefone NÃO — o que fazia
    parecer que a máscara inteira funcionava."""
    padrao = re.search(r"const SEPARADOR = String\.raw`\[([^`]+)\]`", base)
    assert padrao, "o separador tem de estar num lugar só, não repetido por máscara"
    assert "\\s" in padrao.group(1), "espaço tem de contar como fronteira"


@pytest.mark.parametrize("campo", [
    "cnpj", "cpf", "cnpj_cpf", "documento", "doc-titular", "cnpj_cliente",
    "telefone", "celular", "whatsapp", "cep",
])
def test_os_nomes_de_campo_usados_no_erp_sao_reconhecidos(base, campo):
    """Cada um destes existe em alguma tela hoje."""
    separador = re.search(r"const SEPARADOR = String\.raw`(\[[^`]+\])`", base).group(1)
    palavras = re.findall(r'alvoDeMascara\("([^"]+)"\)', base)
    assert palavras, "as máscaras têm de ser declaradas por `alvoDeMascara`"
    assert any(re.search(f"(^|{separador})({p})({separador}|$)", f"pg-0 {campo}", re.I)
               for p in palavras), f"{campo} não seria reconhecido"


def test_campo_de_ARQUIVO_nao_ganha_mascara(base):
    """`documentos`, na tela de lançamento, é o campo de ANEXO — arquivo, não
    número. Máscara ali destruiria o nome do arquivo escolhido."""
    assert ":not([type=file])" in base
    separador = re.search(r"const SEPARADOR = String\.raw`(\[[^`]+\])`", base).group(1)
    palavras = re.findall(r'alvoDeMascara\("([^"]+)"\)', base)
    assert not any(re.search(f"(^|{separador})({p})({separador}|$)", "arq documentos", re.I)
                   for p in palavras), "'documentos' no plural não é documento de pessoa"


def test_tem_saida_para_quem_nao_quer_mascara(base):
    """A exceção fica escrita no lugar onde ela vale, como já vale para a busca
    (`data-sem-busca`)."""
    assert "data-sem-mascara" in base


# ---------------------------------------------------------------------------
# A célula não come mais a informação
# ---------------------------------------------------------------------------
def test_a_celula_mostra_DUAS_linhas_em_vez_de_cortar_numa():
    """*"Tem campo que fica comendo as informações."* Com uma linha só,
    "MATERIAL PARA A FUNDAÇÃO DO BLOCO B" virava "MATERIAL PARA A FUND…" — e o
    que distingue um lançamento do outro está no FIM da frase."""
    css = CSS.read_text(encoding="utf-8")
    bloco = css[css.index("Célula não vira torre"):]
    bloco = bloco[:bloco.index("@media (prefers-reduced-motion")]
    assert "-webkit-line-clamp: 2" in bloco
    assert "white-space: normal" in bloco, "a célula precisa poder quebrar linha"
    assert "text-overflow: ellipsis" not in bloco, "o corte numa linha era o problema"


def test_o_texto_inteiro_fica_no_balaozinho(base):
    """Duas linhas ainda cortam texto muito longo. O resto vai para o `title`,
    mas SÓ quando sobrou — balãozinho repetindo o que já está escrito faz
    ninguém ler nenhum."""
    assert "function explicarCelulasCortadas" in base
    assert "scrollHeight > td.clientHeight" in base


# ---------------------------------------------------------------------------
# A engrenagem da busca
# ---------------------------------------------------------------------------
def test_a_busca_espera_a_pessoa_terminar_de_digitar(base):
    """Era a queixa: *"fica uns segundos meio travado quando tentamos digitar,
    sem aparecer nada. Pensamos que não tá funcionando."*"""
    assert "const ESPERA = 140" in base
    assert "setTimeout(filtrar, ESPERA)" in base


def test_a_busca_tem_teto_e_DIZ_quanto_ficou_de_fora(base):
    """Cortar em silêncio faz a pessoa jurar que o cadastro não existe."""
    assert "const TETO = 60" in base
    assert "e mais ${achados - mostrados}" in base
    assert "nada encontrado com este texto" in base


def test_a_busca_ignora_acento_pontuacao_e_ordem(base):
    """As três coisas que a comparação literal errava: "jose" não achava "JOSÉ";
    colar "11.222.333/0001-81" da nota não achava nada porque o cadastro guarda
    só dígitos; e "silva joao" não achava "JOÃO DA SILVA"."""
    assert "const semAcento" in base and "normalize(\"NFD\")" in base
    assert "termos.every" in base, "cada palavra procurada por conta própria"
    assert "replace(/\\D/g, \"\").includes(digitos)" in base


# ---------------------------------------------------------------------------
# A varredura: nenhuma tela mostra documento cru
# ---------------------------------------------------------------------------
def test_nenhuma_tela_mostra_documento_sem_formatacao():
    """A varredura no navegador (32 telas) achou um caso sobrando na tela de
    Notas. Este teste impede que volte — e pega o próximo que alguém escrever.
    """
    suspeitos = []
    for arq in sorted(TELAS.glob("*.html")):
        texto = arq.read_text(encoding="utf-8")
        for achado in re.finditer(
                r"\$\{(?:escapar\()?([a-z]\w*\.(?:cnpj|cpf|cnpj_cpf|documento|emitente_doc))\)?\}",
                texto):
            linha = texto[:achado.start()].count("\n") + 1
            # A janela olha ANTES e DEPOIS: o `.toLowerCase()` que marca uma
            # busca costuma vir na linha seguinte, e sem olhar para a frente o
            # teste acusava busca legítima como se fosse exibição.
            trecho = texto[max(0, achado.start() - 120):achado.end() + 120]
            # Usar o documento para BUSCAR ou para MANDAR ao servidor é legítimo;
            # o que não pode é aparecer cru na tela.
            if any(p in trecho for p in ("documento(", "toLowerCase", "JSON.stringify",
                                         "body:", "alvo =", "includes(")):
                continue
            suspeitos.append(f"{arq.name}:{linha} → {achado.group(1)}")
    assert not suspeitos, ("documento aparecendo sem formatação:\n  " +
                           "\n  ".join(suspeitos))
