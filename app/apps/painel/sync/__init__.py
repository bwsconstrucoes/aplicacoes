# -*- coding: utf-8 -*-
"""Sincronizacao do painel com o OMIE.

Tres etapas, nesta ordem:
  1. `espelho`  — baixa titulos, movimentos e catalogos do OMIE.
  2. `projetos` — le da planilha "C. Diarios" qual obra pertence a qual projeto.
  3. `fato`     — recalcula a tabela que as telas leem.

Nada aqui e importado quando o Flask sobe: o painel so carrega estes modulos
quando a sincronizacao e realmente disparada. Assim o `pandas` e o `requests`
pesado ficam fora da memoria do processo enquanto ninguem pede atualizacao.
"""
