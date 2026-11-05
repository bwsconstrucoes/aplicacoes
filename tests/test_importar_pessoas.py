# -*- coding: utf-8 -*-
"""IMPORTAR AS PESSOAS DO PIPEFY / DA PLANILHA DO DP.

Decisão do dono em 04/11/2026: *"eu acho que a gente deve colocar só os que
estão ativos"* — ~575 pessoas, não as 3.664 do pipe.

O que estes testes defendem, por ordem de dano:

  1. **CPF errado não entra** — é a identidade da pessoa na folha;
  2. **quem já está no ERP não é sobrescrito** — a planilha não manda por cima
     do que o DP corrigiu na tela;
  3. **só os ativos entram**, como ele decidiu;
  4. **a simulação não escreve nada**;
  5. **coluna não reconhecida APARECE no relatório** — silêncio aqui seria
     perder dado sem ninguém ver.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.importadores import pessoas_planilha as imp

CPF_A = "52998224725"
CPF_B = "11144477735"
CPF_C = "87748248800"
PIS_A = "123.45678.90-0"          # o mesmo número, três grafias
PIS_B = "111.22233.34-0"
PIS_C = "456.12378.90-0"

CABECALHO = (
    "CPF;Nome Completo;Data de Nascimento;Sexo;Estado Civil;Escolaridade;"
    "Nº Identidade;Órgão Emissor (Documento de Identificação);"
    "UF (Documento de Identificação);Nº PIS;Matrícula;"
    "Tipo de Documentação para Cadastro;"
    "Tipo de Contrato;Cargo;Convenção;Código da Obra;Salário;Data de Admissão;"
    "Pagamento;Tipo de CTPS;Solicitar Salário Família?;"
    "Categoria Auxílio Transporte;Valor (Auxílio Transporte);Fase Atual;"
    "Nome do Cônjuge;Nome Filho 1;Data de Nascimento Filho 1;"
    "Jornada de Trabalho;Coluna Que Ninguém Conhece"
)


def linha(cpf="", nome="", **troca) -> str:
    campos = {
        "cpf": cpf, "nome": nome, "nascimento": "10/05/1988", "sexo": "Masculino",
        "estado_civil": "Casado (a)", "escolaridade": "Ensino Médio",
        "rg": "2001123456", "orgao": "SSP", "uf_rg": "CE", "pis": PIS_A,
        "matricula": "", "documentacao": "CTPS", "contrato": "CLT (tempo Indeterminado)",
        "cargo": "PEDREIRO", "convencao": "CE-Fortaleza", "obra": "IMP-01",
        "salario": "2.100,00", "admissao": "01/03/2026", "pagamento": "Mensalista",
        "tipo_ctps": "Digital", "salario_familia": "Sim",
        "cat_transporte": "Vale Transporte", "valor_transporte": "220,00",
        "fase": "Cadastro Concluído", "conjuge": "", "filho": "",
        "nasc_filho": "", "jornada": "Entr.: 07:00h", "esquisita": "qualquer coisa",
    }
    campos.update(troca)
    return ";".join(str(campos[k]) for k in (
        "cpf", "nome", "nascimento", "sexo", "estado_civil", "escolaridade",
        "rg", "orgao", "uf_rg", "pis", "matricula", "documentacao", "contrato", "cargo",
        "convencao", "obra", "salario", "admissao", "pagamento", "tipo_ctps",
        "salario_familia", "cat_transporte", "valor_transporte", "fase",
        "conjuge", "filho", "nasc_filho", "jornada", "esquisita"))


def planilha(*linhas: str) -> bytes:
    return ("\n".join((CABECALHO,) + linhas)).encode("utf-8")


# ===========================================================================
# SEM BANCO: o de-para e as traduções, que é onde o erro nasce
# ===========================================================================
def test_a_coluna_e_reconhecida_pelo_nome_sem_acento_e_sem_caixa():
    achadas, sobraram = imp._achar_colunas({
        "nome completo": "", "DATA DE NASCIMENTO": "", "nº pis": "",
        "orgao emissor": "", "coluna estranha": ""})
    assert achadas["nome"] == "nome completo"
    assert achadas["data_nascimento"] == "DATA DE NASCIMENTO"
    assert achadas["pis"] == "nº pis"
    assert achadas["rg_orgao"] == "orgao emissor"
    assert sobraram == ["coluna estranha"]


def test_os_tres_jeitos_de_chamar_a_mesma_coluna_caem_no_mesmo_campo():
    """O rótulo do pipe, o cabeçalho da aba e o nome do ERP."""
    for titulo in ("Nome Completo", "nome", "Nome do Colaborador"):
        achadas, _ = imp._achar_colunas({titulo: ""})
        assert achadas.get("nome") == titulo


def test_duas_colunas_nao_disputam_o_mesmo_campo():
    """"CNPJ para Admissão" e "CNPJ" são campos diferentes, e a planilha tem
    as duas — se disputassem, a razão social da PJ levaria o CNPJ da matriz."""
    achadas, sobraram = imp._achar_colunas(
        {"CNPJ para Admissão": "", "CNPJ": "", "Razão Social": ""})
    assert achadas["cnpj_admissao"] == "CNPJ para Admissão"
    assert achadas["cnpj_pj"] == "CNPJ"
    assert sobraram == []


@pytest.mark.parametrize("campo, rotulo, esperado", [
    ("sexo", "Masculino", "M"),
    ("sexo", "FEMININO", "F"),
    ("vinculo", "Prestador de Serviço", "PRESTADOR"),
    ("vinculo", "CTPS", "CTPS"),
    ("estado_civil", "Solteiro (a)", "SOLTEIRO"),
    ("estado_civil", "União Estável", "UNIAO_ESTAVEL"),
    ("escolaridade", "Pós-graduação", "POS"),
    ("tipo_contrato", "CLT (tempo Indeterminado)", "CLT_INDETERMINADO"),
    ("tipo_contrato", "Autônomo (RPA)", "AUTONOMO_RPA"),
    ("categoria_transporte", "Diário e Vale Transporte", "DIARIO_E_VT"),
    ("categoria_transporte", "Cartão", "CARTAO"),
    ("categoria_alimentacao", "Segunda à Quinta", "SEG_QUI"),
    ("meio_transporte", "Transporte Coletivo", "COLETIVO"),
    ("contrato_experiencia", "45 dias, prorrogável por mais 45 dias", "45+45 dias"),
])
def test_o_rotulo_do_pipe_vira_o_valor_do_erp(campo, rotulo, esperado):
    assert imp._traduzir(campo, rotulo) == esperado


def test_valor_que_nao_existe_na_lista_devolve_nada_em_vez_de_chutar():
    """Devolver o texto cru faria o banco recusar a linha inteira; chutar o
    primeiro da lista gravaria "Solteiro" em quem é casado. Vira AVISO."""
    assert imp._traduzir("estado_civil", "Amigado") is None
    assert imp._traduzir("tipo_contrato", "Contrato Novo") is None


def test_a_empresa_vem_com_o_cnpj_colado_no_nome():
    assert imp._cnpj("00.079.526/0001-09 - BWS") == "00079526000109"
    assert imp._cnpj("BWS") == ""


def test_as_colunas_que_o_erp_nao_guarda_tem_motivo_escrito():
    """"Jornada de Trabalho" ser ignorada é DIFERENTE de não ser reconhecida —
    a primeira é decisão, a segunda é dado perdido."""
    assert "jornada de trabalho" in imp.IGNORADAS_DE_PROPOSITO
    assert "salario extenso" in imp.IGNORADAS_DE_PROPOSITO


def test_todo_campo_do_de_para_existe_no_cadastro_ou_e_tratado_a_mao():
    """Trava: campo no de-para que o `salvar()` não conhece gravaria nada, em
    silêncio. Os tratados à mão estão listados aqui de propósito."""
    from app.apps.erp.core.cadastros import pessoas as svc
    conhecidos = set(svc.TEXTOS) | set(svc.DATAS) | set(svc.DINHEIRO) \
        | set(svc.BOOLEANOS) | set(svc.LIGACOES) | set(svc.DA_LISTA) \
        | {"cpf", "nome", "situacao", "regime"}
    a_mao = {"cargo", "cbo", "convencao", "obra", "empresa", "fase", "diversos",
             "conjuge_nome", "conjuge_nascimento", "conjuge_cpf", "responsavel"}
    a_mao |= {f"filho{i}_{p}" for i in range(1, 6) for p in ("nome", "nascimento")}
    sobrando = set(imp.COLUNAS) - conhecidos - a_mao
    assert not sobrando, f"campos no de-para que ninguém grava: {sorted(sobrando)}"


# ===========================================================================
# COM BANCO
# ===========================================================================
banco_ = pytest.mark.banco


@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="DP da importação", email="dp@importar.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL, telefone="5585900000001")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@pytest.fixture
def cenario(sessao_real, dono):
    """A obra e a convenção que a planilha cita já existem; o cargo também."""
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    from app.apps.erp.db.models.cadastros import Obra
    obra = Obra(codigo="IMP-01", nome="Obra da importação", status="ATIVA")
    sessao_real.add(obra)
    svc_cargos.achar_ou_criar_cargo(sessao_real, "PEDREIRO", cbo="7152-10")
    svc_cargos.achar_ou_criar_convencao(sessao_real, "CE-Fortaleza")
    sessao_real.flush()
    return obra


@banco_
def test_a_pessoa_entra_com_os_campos_traduzidos(sessao_real, dono, cenario):
    from app.apps.erp.db.models.cadastros import Colaborador
    r = imp.importar(sessao_real, planilha(linha(CPF_A, "Maria da Silva Souza")),
                     gravar=True, usuario=dono)
    assert r["criadas"] == 1 and r["rejeitadas"] == []
    c = sessao_real.query(Colaborador).filter_by(cpf=CPF_A).one()
    assert c.nome == "Maria da Silva Souza"
    assert (c.sexo, c.estado_civil, c.escolaridade) == ("M", "CASADO", "MEDIO")
    assert c.vinculo == "CTPS" and c.tipo_contrato == "CLT_INDETERMINADO"
    assert c.categoria_transporte == "VALE_TRANSPORTE"
    assert str(c.aux_transporte) == "220.00" and str(c.salario) == "2100.00"
    assert c.obra_id == cenario.id and c.cargo_id is not None
    assert c.ctps_digital is True and c.mensalista is True
    assert c.solicitar_salario_familia is True
    assert c.data_nascimento.isoformat() == "1988-05-10"
    assert c.admissao.isoformat() == "2026-03-01"


@banco_
def test_o_primeiro_vinculo_nasce_com_a_importacao(sessao_real, dono, cenario):
    """Sem ele a primeira readmissão não teria o que fechar."""
    from app.apps.erp.core.cadastros import pessoas as svc
    imp.importar(sessao_real, planilha(linha(CPF_A, "Maria da Silva Souza")),
                 gravar=True, usuario=dono)
    from app.apps.erp.db.models.cadastros import Colaborador
    c = sessao_real.query(Colaborador).filter_by(cpf=CPF_A).one()
    v = svc.vinculo_aberto(sessao_real, c.id)
    assert v is not None and v.admissao.isoformat() == "2026-03-01"


@banco_
def test_cpf_em_branco_ou_torto_e_recusado_com_o_numero_da_linha(
        sessao_real, dono, cenario):
    r = imp.importar(sessao_real, planilha(
        linha("", "Sem Documento Nenhum"),
        linha("111.111.111-11", "CPF Repetido Demais"),
        linha(CPF_A, "Entra Normalmente Aqui")), gravar=True, usuario=dono)
    assert r["criadas"] == 1
    assert [x["linha"] for x in r["rejeitadas"]] == [2, 3]
    assert all("dígitos" in x["motivo"] for x in r["rejeitadas"])


@banco_
def test_so_os_ativos_entram(sessao_real, dono, cenario):
    """A decisão dele: os ~2.960 desligados ficam na planilha."""
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Fica Porque Trabalha"),
        linha(CPF_B, "Saiu Em Janeiro", pis=PIS_B, fase="Colaboradores Desligados"),
        linha(CPF_C, "Cadastro Cancelado", pis=PIS_C, fase="Cadastro Cancelado"),
    ), gravar=True, usuario=dono)
    assert r["criadas"] == 1 and r["fora"] == 2


@banco_
def test_com_so_ativos_desligado_o_desligado_tambem_entra(sessao_real, dono, cenario):
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Fica Porque Trabalha"),
        linha(CPF_B, "Saiu Em Janeiro", pis=PIS_B, fase="Colaboradores Desligados"),
    ), gravar=True, so_ativos=False, usuario=dono)
    assert r["criadas"] == 2 and r["fora"] == 0


@banco_
def test_quem_ja_esta_no_erp_nao_e_sobrescrito(sessao_real, dono, cenario):
    """O DP corrigiu o nome na tela; a planilha do Pipefy ainda tem o antigo."""
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import Colaborador
    svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Maria de Souza Correto",
                            "matricula": "999"}, dono)
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "MARIA DE SOUSA ERRADO", matricula="123", salario="3.000,00")),
        gravar=True, usuario=dono)
    assert r["criadas"] == 0 and r["ja_existiam"] == 1
    c = sessao_real.query(Colaborador).filter_by(cpf=CPF_A).one()
    assert c.nome == "Maria de Souza Correto"
    assert c.matricula == "999"
    o_que = r["divergencias"][0]["o_que"]
    assert any("nome" in x for x in o_que)
    assert any("matrícula" in x for x in o_que)


@banco_
def test_a_simulacao_nao_grava_nada(sessao_real, dono, cenario):
    from app.apps.erp.db.models.cadastros import Colaborador
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Maria da Silva Souza"),
        linha(CPF_B, "Jose Pereira Lima", pis=PIS_B, cargo="CARGO QUE NAO EXISTE")),
        gravar=False, usuario=dono)
    assert r["criadas"] == 2 and r["gravou"] is False
    assert r["cargos_novos"] == ["CARGO QUE NAO EXISTE"]
    assert sessao_real.query(Colaborador).filter_by(cpf=CPF_A).first() is None
    from app.apps.erp.db.models.cadastros import Cargo
    assert not [c for c in sessao_real.query(Cargo).all()
                if c.nome == "CARGO QUE NAO EXISTE"]


@banco_
def test_a_coluna_nao_reconhecida_aparece_no_relatorio(sessao_real, dono, cenario):
    """Silêncio aqui seria perder dado sem ninguém ver."""
    r = imp.importar(sessao_real, planilha(linha(CPF_A, "Maria da Silva Souza")),
                     gravar=False, usuario=dono)
    assert any("ninguém conhece" in c.lower()
               for c in r["colunas_nao_reconhecidas"])
    assert any("jornada" in x.lower() for x in r["colunas_ignoradas"])
    assert r["colunas_lidas"]["cpf"] == "cpf"


@banco_
def test_obra_que_nao_existe_no_erp_vira_aviso_e_a_pessoa_entra_sem_obra(
        sessao_real, dono, cenario):
    from app.apps.erp.db.models.cadastros import Colaborador
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Maria da Silva Souza", obra="OBRA-FANTASMA")),
        gravar=True, usuario=dono)
    assert r["criadas"] == 1 and r["obras_nao_achadas"] == ["OBRA-FANTASMA"]
    c = sessao_real.query(Colaborador).filter_by(cpf=CPF_A).one()
    assert c.obra_id is None


@banco_
def test_a_obra_tambem_e_achada_pelo_nome(sessao_real, dono, cenario):
    """O connector do Pipefy devolve às vezes o nome, não o código."""
    from app.apps.erp.db.models.cadastros import Colaborador
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Maria da Silva Souza", obra="obra da importação")),
        gravar=True, usuario=dono)
    assert r["obras_nao_achadas"] == []
    c = sessao_real.query(Colaborador).filter_by(cpf=CPF_A).one()
    assert c.obra_id == cenario.id


@banco_
def test_valor_fora_da_lista_vira_aviso_e_o_campo_fica_vazio(
        sessao_real, dono, cenario):
    from app.apps.erp.db.models.cadastros import Colaborador
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Maria da Silva Souza", estado_civil="Amigado")),
        gravar=True, usuario=dono)
    assert r["criadas"] == 1
    assert any("Amigado" in a for a in r["avisos"])
    assert sessao_real.query(Colaborador).filter_by(cpf=CPF_A).one().estado_civil is None


@banco_
def test_conjuge_e_filhos_viram_dependentes(sessao_real, dono, cenario):
    """No pipe eram campos soltos — cônjuge e até cinco filhos."""
    from app.apps.erp.db.models.cadastros import Colaborador, ColaboradorDependente
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Maria da Silva Souza", conjuge="Antonio Souza Filho",
              filho="Pedro Souza da Silva", nasc_filho="12/07/2019")),
        gravar=True, usuario=dono)
    assert r["dependentes"] == 2
    c = sessao_real.query(Colaborador).filter_by(cpf=CPF_A).one()
    deps = sessao_real.query(ColaboradorDependente).filter_by(colaborador_id=c.id).all()
    assert {d.parentesco for d in deps} == {"CONJUGE", "FILHO"}
    filho = next(d for d in deps if d.parentesco == "FILHO")
    assert filho.data_nascimento.isoformat() == "2019-07-12"


@banco_
def test_o_mesmo_cpf_duas_vezes_na_planilha_entra_uma_so(sessao_real, dono, cenario):
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Maria da Silva Souza"),
        linha(CPF_A, "Maria da Silva Souza"),
    ), gravar=True, usuario=dono)
    assert r["criadas"] == 1
    assert any("de novo na planilha" in a for a in r["avisos"])


@banco_
def test_planilha_sem_a_coluna_do_cpf_e_recusada_inteira(sessao_real, dono):
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    with pytest.raises(ErroValidacao, match="coluna do CPF"):
        imp.importar(sessao_real, b"Nome Completo;Cargo\nJoao;PEDREIRO\n",
                     gravar=False, usuario=dono)


@banco_
def test_convencao_que_nao_existe_avisa_para_importar_cargos_antes(
        sessao_real, dono, cenario):
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Maria da Silva Souza", convencao="SP-Campinas")),
        gravar=True, usuario=dono)
    assert any("planilha de cargos primeiro" in a for a in r["avisos"])


@banco_
def test_pis_repetido_na_planilha_recusa_a_segunda_linha_sem_derrubar_a_terceira(
        sessao_real, dono, cenario):
    """DEFEITO ACHADO FAZENDO: o PIS é único no banco (migração 086) e a planilha
    do Pipefy repete — foi digitado à mão por anos. Antes, o banco estourava no
    meio da importação e TODAS as linhas seguintes falhavam em cascata, por um
    motivo que não era o delas. Agora cada pessoa entra no seu próprio ponto de
    retorno, e o PIS repetido é recusado com o nome do outro cadastro."""
    r = imp.importar(sessao_real, planilha(
        linha(CPF_A, "Maria da Silva Souza", pis=PIS_A),
        linha(CPF_B, "Jose Pereira Lima", pis=PIS_A),
        linha(CPF_C, "Antonio Gomes Neto", pis=PIS_C),
    ), gravar=True, usuario=dono)
    assert r["criadas"] == 2
    assert [x["linha"] for x in r["rejeitadas"]] == [3]
    assert "Maria da Silva Souza" in r["rejeitadas"][0]["motivo"]
    from app.apps.erp.db.models.cadastros import Colaborador
    assert sessao_real.query(Colaborador).filter_by(cpf=CPF_C).first() is not None


@banco_
def test_pis_que_ja_esta_em_outra_pessoa_do_erp_e_recusado(sessao_real, dono, cenario):
    from app.apps.erp.core.cadastros import pessoas as svc
    svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Jose Pereira Lima",
                             "pis": PIS_A}, dono)
    r = imp.importar(sessao_real, planilha(linha(CPF_A, "Maria da Silva Souza",
                                                 pis=PIS_A)),
                     gravar=True, usuario=dono)
    assert r["criadas"] == 0
    assert "Jose Pereira Lima" in r["rejeitadas"][0]["motivo"]


@banco_
def test_planilha_excel_com_titulo_em_cima_da_tabela(sessao_real, dono, cenario):
    """Planilha de gente tem título, logotipo e painel de filtro em cima da
    tabela. Tomar a primeira linha como cabeçalho leria colunas chamadas
    "Relatório de Colaboradores" e "" — e a importação inteira se perderia."""
    import io

    from openpyxl import Workbook
    pasta = Workbook()
    aba = pasta.active
    aba.append(["RELATÓRIO DE COLABORADORES ATIVOS", None, None])
    aba.append([None, None, None])
    aba.append(["CPF", "Nome Completo", "Cargo"])
    aba.append([CPF_A, "Maria da Silva Souza", "PEDREIRO"])
    buf = io.BytesIO()
    pasta.save(buf)
    r = imp.importar(sessao_real, buf.getvalue(), gravar=True, usuario=dono)
    assert r["criadas"] == 1 and r["rejeitadas"] == []


@banco_
def test_cabecalho_escrito_de_outro_jeito_nao_devolve_planilha_vazia(
        sessao_real, dono, cenario):
    """"A planilha veio vazia" é a pior mensagem possível: não diz nada. Quando
    o cabeçalho não traz "CPF" escrito assim, a leitura cai no jeito antigo
    (primeira aba, primeira linha) e o erro passa a DIZER o que leu."""
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    with pytest.raises(ErroValidacao) as e:
        imp.importar(sessao_real, "Documento;Nome\n123;Joao\n".encode("utf-8"),
                     gravar=False, usuario=dono)
    assert "coluna do CPF" in str(e.value)
    assert "documento" in str(e.value).lower()
