"""Todo catálogo que se instala uma vez tem de ter BOTÃO em alguma tela.

O defeito que este arquivo existe para impedir apareceu em 10/09/2026, quando o
dono abriu o Arquivo em produção pela primeira vez e leu:

    "O catálogo de tipos de documento está vazio — instale o catálogo antes de
     usar a leitura automática."

A rota de instalar existia desde que o Arquivo foi feito. **Nenhuma tela a
chamava.** A mensagem mandava fazer uma coisa que não havia como fazer, e a
tela inteira ficava inútil até alguém mexer no banco por fora. O mesmo valia
para os conjuntos prontos e para os tipos de medição.

É a mesma família dos testes que já existem — `test_telas_chamam_rota_que_
existe` (a tela pede endereço que o servidor tem) —, só que pelo outro lado:
o servidor oferece uma instalação e ALGUÉM precisa conseguir apertá-la.
"""
from __future__ import annotations

import pathlib
import re

TELAS = pathlib.Path("app/apps/erp/templates")
ROTAS = pathlib.Path("app/apps/erp/routes.py")

# Endereço de instalação = rota POST que termina em /aplicar. É a convenção do
# ERP para "traz o catálogo padrão", e é o que precisa de botão.
PADRAO = re.compile(r'@bp\.route\("(/erp/api/[^"]*?/aplicar)"[^)]*methods=\[[^\]]*"POST"')


def _rotas_de_instalacao() -> list[str]:
    return sorted(set(PADRAO.findall(ROTAS.read_text(encoding="utf-8"))))


def _texto_das_telas() -> str:
    return "\n".join(p.read_text(encoding="utf-8")
                     for p in sorted(TELAS.glob("*.html")))


def test_existe_ao_menos_uma_rota_de_instalacao():
    """Se este teste falhar, a convenção mudou e o resto aqui virou letra
    morta — o que é pior que não ter teste nenhum."""
    assert _rotas_de_instalacao(), "nenhuma rota */aplicar encontrada"


def test_toda_instalacao_tem_botao_em_alguma_tela():
    telas = _texto_das_telas()
    sem_botao = [r for r in _rotas_de_instalacao() if r not in telas]

    assert sem_botao == [], (
        f"estas instalações não têm botão em tela nenhuma: {sem_botao}. "
        f"Rota que ninguém chama é catálogo que ninguém instala — e a tela que "
        f"depende dele fica travada pedindo o que não há como fazer.")
