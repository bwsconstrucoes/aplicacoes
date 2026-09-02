# ============================================================================
# BWS ERP — scripts/seed_usuarios_teste.py
# Cria os quatro operadores de teste usados para conferir, na tela, o que cada
# perfil enxerga e o que lhe é recusado.
#
# Uso:  python app/apps/erp/scripts/seed_usuarios_teste.py CODIGO_DA_OBRA
#
# O administrativo e o supervisor ficam presos à obra informada — é isso que
# torna o teste de escopo honesto: com uma obra só, dá para abrir um título de
# OUTRA obra e conferir que o sistema responde "não encontrado".
#
# A senha de cada um é sorteada na hora e impressa UMA vez, no fim. Nada de
# senha escrita no código: é exatamente o hábito que nos custou uma troca de
# credencial. Anote no gerenciador de senhas e troque depois do teste.
#
# Rodar de novo não duplica ninguém: quem já existe é pulado, com aviso.
# ============================================================================
from __future__ import annotations

import os
import secrets
import sys

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")))

from sqlalchemy import select  # noqa: E402

from app.apps.erp.core.auth.service import criar_usuario  # noqa: E402
from app.apps.erp.db.database import get_session  # noqa: E402
from app.apps.erp.db.models.cadastros import (  # noqa: E402
    EscopoVisao, Obra, Usuario, UsuarioObra,
)

# nome, e-mail, perfil, prende à obra?
OPERADORES = [
    ("TESTE — Administrativo de obra", "teste.administrativo@bws.local",
     "ADMINISTRATIVO_OBRA", True),
    ("TESTE — Supervisor de obra", "teste.supervisor@bws.local",
     "SUPERVISOR_OBRA", True),
    ("TESTE — Financeiro", "teste.financeiro@bws.local",
     "FINANCEIRO", False),
    ("TESTE — Departamento pessoal", "teste.dp@bws.local",
     "DEPARTAMENTO_PESSOAL", False),
]


def main() -> None:
    if len(sys.argv) < 2:
        print("Informe o código da obra à qual prender o administrativo e o "
              "supervisor.\nExemplo:  python app/apps/erp/scripts/"
              "seed_usuarios_teste.py OB-001")
        raise SystemExit(1)
    codigo_obra = sys.argv[1].strip()

    criados: list[tuple[str, str, str]] = []
    with get_session() as s:
        obra = s.scalars(select(Obra).where(Obra.codigo == codigo_obra)).first()
        if obra is None:
            disponiveis = ", ".join(o.codigo for o in s.scalars(
                select(Obra).order_by(Obra.codigo)).all()) or "(nenhuma)"
            print(f"Obra {codigo_obra!r} não encontrada.\nObras: {disponiveis}")
            raise SystemExit(1)

        for nome, email, perfil, prender in OPERADORES:
            if s.scalars(select(Usuario).where(Usuario.email == email)).first():
                print(f"já existe, pulando: {email}")
                continue
            senha = secrets.token_urlsafe(9)          # provisória, para trocar
            u = criar_usuario(s, nome=nome, email=email, senha=senha, perfil=perfil)
            # Todo mundo começa no mais restritivo — inclusive no teste. Ampliar
            # o administrativo é justamente o que se quer exercitar na tela.
            u.escopo_visao = EscopoVisao.PROPRIOS
            if prender:
                s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id))
            criados.append((email, senha, perfil))
        s.commit()

    if not criados:
        print("\nNenhum operador novo. Os de teste já estavam cadastrados.")
        return
    # `obra` já saiu da sessão aqui; o código veio do argumento e é o mesmo.
    print(f"\n=== Operadores de teste — obra {codigo_obra} ===")
    print("Anote agora: a senha não é exibida de novo.\n")
    largura = max(len(e) for e, _, _ in criados)
    for email, senha, perfil in criados:
        print(f"  {email:<{largura}}  {senha}   ({perfil})")
    print("\nTodos começam vendo apenas o que eles mesmos lançarem.")
    print("Para testar o escopo ampliado: Configurações > Operadores >")
    print("TESTE — Administrativo de obra > 'O que esta pessoa enxerga' >")
    print("'Tudo das obras designadas'.")


if __name__ == "__main__":
    main()
