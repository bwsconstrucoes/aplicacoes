# Telas originais em Streamlit — só referência

Estes três arquivos são as telas como rodavam no computador, em Streamlit. Eles
**não são executados** pelo sistema online e não são importados por nada.

Ficam aqui por um motivo só: são a fonte da verdade das regras de cálculo
enquanto a conversão para HTML não termina. Cada tela portada deve bater número
por número com a original.

- `telas_streamlit.py` — Visão Geral, DRE, Fluxo de Caixa, Resultado por Obra/Projeto,
  Comprometido vs Executado, Necessidade de Caixa.
- `prestacao_contas.py` — rateio administrativo e divisão entre sócios.
- `relatorio_pdf.py` — geração do PDF do DRE.

Quando todas as telas estiverem convertidas e conferidas, esta pasta sai.

O arquivo das telas chamava-se `app.py`. Foi renomeado por segurança: um
`app.py` solto numa pasta pode **sombrear o pacote `app`** do projeto se essa
pasta cair no caminho de busca do Python, e aí o sistema inteiro importa a coisa
errada. Aconteceu comigo numa conferência; melhor o nome do que a surpresa.
