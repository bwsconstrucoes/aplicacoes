# ============================================================================
# ERP — scripts/resetar_senha.py
# Redefine a senha de um usuário existente (uso administrativo).
# Rodar no Shell do serviço no Render:
#     python app/apps/erp/scripts/resetar_senha.py
# ============================================================================
from __future__ import annotations

import getpass
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")))

from sqlalchemy import select

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.database import get_session
from app.apps.erp.db.models.cadastros import Usuario


def main() -> None:
    print("=== ERP BWS — redefinir senha ===")
    with get_session() as s:
        usuarios = s.scalars(select(Usuario).order_by(Usuario.id)).all()
        if not usuarios:
            print("Nenhum usuário cadastrado. Use o seed_admin.py.")
            return
        print("Usuários cadastrados:")
        for u in usuarios:
            print(f"  [{u.id}] {u.email} — {u.nome} ({u.perfil.value}, "
                  f"{'ativo' if u.ativo else 'INATIVO'})")
        email = input("\nE-mail do usuário: ").strip().lower()
        usuario = s.scalars(select(Usuario).where(Usuario.email == email)).first()
        if usuario is None:
            print(f"ERRO: nenhum usuário com o e-mail {email}.")
            return
        senha = getpass.getpass("Nova senha (mín. 8 caracteres): ")
        if senha != getpass.getpass("Confirme a nova senha: "):
            print("ERRO: as senhas não conferem.")
            return
        try:
            usuario.senha_hash = gerar_hash(senha)
        except ValueError as e:
            print(f"ERRO: {e}")
            return
        usuario.ativo = True
        s.commit()
        print(f"\nSenha redefinida para {usuario.nome} ({usuario.email}).")
        print("Já pode entrar em /erp com essa senha.")


if __name__ == "__main__":
    main()
