# ============================================================================
# BWS ERP — core/auth/service.py
# Autenticação com PBKDF2-SHA256 (stdlib).
# ============================================================================
from __future__ import annotations

import hashlib
import hmac
import os
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario

_ITERACOES = 240_000


class ErroAutenticacao(Exception):
    """Credenciais inválidas ou usuário inativo."""


def gerar_hash(senha: str) -> str:
    if not senha or len(senha) < 8:
        raise ValueError("Senha deve ter no mínimo 8 caracteres.")
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", senha.encode("utf-8"), salt, _ITERACOES)
    return f"pbkdf2${_ITERACOES}${salt.hex()}${dk.hex()}"


def verificar_senha(senha: str, hash_armazenado: str) -> bool:
    try:
        algoritmo, iteracoes, salt_hex, hash_hex = hash_armazenado.split("$")
        if algoritmo != "pbkdf2":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", senha.encode("utf-8"),
                                 bytes.fromhex(salt_hex), int(iteracoes))
        return hmac.compare_digest(dk.hex(), hash_hex)
    except (ValueError, AttributeError):
        return False


def autenticar(s: Session, email: str, senha: str) -> Usuario:
    email = (email or "").strip().lower()
    usuario = s.scalars(select(Usuario).where(Usuario.email == email)).first()
    if usuario is None or not usuario.ativo or not verificar_senha(senha or "", usuario.senha_hash):
        raise ErroAutenticacao("E-mail ou senha inválidos.")
    return usuario


def criar_usuario(s: Session, *, nome: str, email: str, senha: str,
                  perfil: str = "CONSULTA", criado_por: Optional[Usuario] = None) -> Usuario:
    if criado_por is not None and criado_por.perfil != PerfilUsuario.ADMIN:
        raise PermissionError("Apenas ADMIN cria usuários.")
    email = (email or "").strip().lower()
    if "@" not in email:
        raise ValueError(f"E-mail inválido: {email!r}")
    perfil = (perfil or "").strip().upper()
    if perfil not in {p.value for p in PerfilUsuario}:
        raise ValueError(f"Perfil inválido: {perfil!r}")
    if s.scalars(select(Usuario).where(Usuario.email == email)).first() is not None:
        raise ValueError(f"Já existe usuário com o e-mail {email}.")
    usuario = Usuario(nome=(nome or "").strip(), email=email,
                      senha_hash=gerar_hash(senha), perfil=PerfilUsuario(perfil))
    s.add(usuario)
    s.flush()
    return usuario
