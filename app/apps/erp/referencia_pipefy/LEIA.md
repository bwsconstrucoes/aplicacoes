# Referência: os dois pipes do Pipefy, campo por campo

Extraído em 04/11/2026 dos arquivos que o dono enviou (exportação da API do
Pipefy). **É material de consulta, não de execução** — a decisão do que fica,
do que muda e do que sai está em `../CADASTRO_DE_PESSOAS.md`.

- `pipe_cadastro_colaboradores.txt` — pipe 301487297, 3.664 cards: fases,
  etiquetas, os 51 campos do formulário de abertura e os 94 campos das fases,
  com tipo, obrigatoriedade e as listas de opções.
- `pipe_solicitacoes_dp.txt` — pipe 301514316, 80.992 cards: idem, com os 106
  campos do formulário e os 28 tipos de solicitação.

**Por que ficam versionados:** o Pipefy vai ser desligado. Quando ele morrer,
esta é a única descrição do que o DP coletava — e vai ser consultada toda vez
que faltar um campo no ERP. Os JSONs originais (2,5 MB, com dados da
organização e dos membros) **não** entram no repositório.

As listas de opções daqui são para **reusar**: escolaridade, estado civil, órgão
emissor, tipo de contrato, contrato de experiência, categorias de auxílio. Elas
já estão acertadas com o que a folha espera.
