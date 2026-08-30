# ============================================================================
# BWS ERP — scripts/seed_admin.py
# Cria o usuário ADMIN inicial e o usuário técnico 'sistema' (integrações).
# Uso:  python scripts/seed_admin.py
# Solicita e-mail e senha interativamente (senha não fica em histórico).
# ============================================================================
from __future__ import annotations

import getpass
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")))

import secrets

from app.apps.erp.core.auth.service import criar_usuario
from app.apps.erp.db.database import get_session


def main() -> None:
    print("=== BWS ERP — criação do administrador inicial ===")
    nome = input("Nome completo: ").strip()
    email = input("E-mail de login: ").strip()
    senha = getpass.getpass("Senha (mín. 8 caracteres): ")
    confirma = getpass.getpass("Confirme a senha: ")
    if senha != confirma:
        print("ERRO: senhas não conferem.")
        return

    with get_session() as s:
        admin = criar_usuario(s, nome=nome, email=email, senha=senha, perfil="ADMIN")
        # Usuário técnico p/ atribuição de eventos vindos da API/robôs
        criar_usuario(
            s, nome="Sistema (integrações)", email="sistema@bws.local",
            senha=secrets.token_urlsafe(24), perfil="CONSULTA",
        )
        s.commit()
        print(f"ADMIN criado: {admin.email} (id {admin.id})")
        print("Usuário técnico 'sistema@bws.local' criado (sem login humano).")


if __name__ == "__main__":
    main()
