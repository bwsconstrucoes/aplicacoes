"""O aval fora do escopo tem de responder "não encontrado", não "sem permissão".

A fila de aval já era filtrada — o supervisor nunca VIA na tela o título de
outra obra. Mas quem mandasse o número direto recebia "este título não é de uma
obra sob sua supervisão", resposta que CONFIRMA que o título existe. Varrer os
números mapearia os lançamentos das outras obras sem abrir nenhum.

Achado na segunda parte da varredura adversarial, em 11/09/2026.

COM BANCO DE VERDADE porque o recorte vive no `WHERE`.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.db.models.cadastros import (
    Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)

pytestmark = pytest.mark.banco


def test_aval_fora_do_escopo_responde_igual_a_inexistente(sessao_real):
    """A falha: o supervisor que mandasse o número de um título de outra obra
    recebia "este título não é de uma obra sob sua supervisão" — resposta que
    CONFIRMA que o título existe. Varrer os números mapearia os lançamentos
    das outras obras sem abrir nenhum.
    """
    from datetime import date
    from decimal import Decimal

    from app.apps.erp.core.titulos import aval
    from app.apps.erp.db.models.cadastros import (
        Categoria, Fornecedor, RegimeTributario, TipoPessoa)
    from app.apps.erp.db.models.financeiro import (
        FormaPagamento, Rateio, StatusTitulo, Titulo, TipoTitulo)

    s = sessao_real
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="CONSTRUTORA ALFA LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="2.1.01", descricao="Material")
    lancou = Usuario(nome="Adm", email="adm2@teste.bws.local",
                     senha_hash=gerar_hash("senha-de-teste-1234"),
                     perfil=P.ADMINISTRATIVO_OBRA)
    supervisor = Usuario(nome="Supervisor da creche",
                         email="sup2@teste.bws.local",
                         senha_hash=gerar_hash("senha-de-teste-1234"),
                         perfil=P.SUPERVISOR_OBRA)
    s.add_all([creche, escola, forn, cat, lancou, supervisor])
    s.flush()
    s.add(UsuarioObra(usuario_id=supervisor.id, obra_id=creche.id))
    s.flush()

    def titulo(sp, obra):
        t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
                   fornecedor_id=forn.id, descricao="compra",
                   valor_bruto=Decimal("500"), valor_liquido=Decimal("500"),
                   competencia=date(2026, 9, 1), categoria_id=cat.id,
                   forma_pagamento=FormaPagamento.PIX,
                   status=StatusTitulo.AGUARDANDO_AVAL,
                   solicitante_id=lancou.id)
        s.add(t)
        s.flush()
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=Decimal("500"),
                     percentual=Decimal("100.0000")))
        s.flush()
        return t

    da_escola = titulo("SP-ESC-1", escola)
    da_creche = titulo("SP-CRE-1", creche)

    with pytest.raises(ErroNaoEncontrado) as fora:
        aval.registrar(s, da_escola.id, supervisor)
    with pytest.raises(ErroNaoEncontrado) as inexistente:
        aval.registrar(s, 99999999, supervisor)
    assert str(fora.value) == str(inexistente.value)

    # e o que é dele continua sendo assinável
    rel = aval.registrar(s, da_creche.id, supervisor)
    assert rel
