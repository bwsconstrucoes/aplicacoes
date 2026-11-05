# Referência: o que os cenários do Make faziam

Levantado em 04 e 05/11/2026, dos **quatro cenários** que o dono enviou. Os
pipes têm **nove webhooks** para o Make; estes quatro são os que vieram, e
depois da leitura do `Card Movido` eles **cobrem o cadastro inteiro** — o que
faltava era quem criava a linha da planilha, e é ele.

**É material de consulta.** A decisão do que fica, do que muda e do que sai está
em `../CADASTRO_DE_PESSOAS.md`.

| Cenário | Módulos | O que ele é |
|---|---|---|
| `Cadastro de Colaboradores - Novo Card` | — | procura o CPF na planilha; se existe, avisa e **apaga o card**; se não, avisa e cadastra no MobPonto |
| `Cadastro de Colaboradores - Card Movido` | **190** | o grande: **cria** a linha da planilha, arquiva os anexos no Dropbox, cadastra/inativa no MobPonto, avisa por WhatsApp e Telegram, escreve a planilha de geração de documento e, no cancelamento, apaga tudo |
| `Cadastro de Colaboradores - Botão` | — | a ação `atualizarcadastro`: reescreve as **74 colunas** da aba "Dados Documentos". É o contrato que a folha lê |
| `SDP - Novo Card` / `SDP - Card Concluído` | 21 roteadores | a fila de solicitações: roteia por tipo e escreve de volta no card |

## O que o `Card Movido` escreve, e onde

- **aba "Dados Documentos"** da planilha `1fqi4QUO…` — `addRow` com 50 colunas
  no nascimento, `updateRow` com as 74 depois. **As mesmas 74 do Botão**: a
  criação não escreve nenhuma coluna que a atualização não escreva.
- **aba "Dados Gerais"** da mesma planilha — uma célula com o CPF, que serve de
  chave para a geração de documento.
- **planilha "Planilha de Cadastro de Funcionários"** (`1iqJwLwn…`), 20 colunas:
  nome, nascimento, CPF, identidade com órgão/UF/emissão, nome da mãe, sexo,
  admissão, endereço completo, telefone e tipo de contrato. Também para gerar
  documento.
- **planilha de Log** (`1MTxfiUu…`) — uma linha por execução.

## Onde os papéis ficavam: DROPBOX

`/BWS DP/FUNCIONÁRIOS/<Nome Completo>` — uma pasta por pessoa, com os seis
anexos obrigatórios (identificação, CTPS foto, CTPS informações, CPF,
comprovante de endereço, foto do colaborador). Crachás em
`/BWS DP/DOCUMENTOS/CRACHÁS`.

⚠️ **No ERP a regra é outra, e é dele (05/11/2026):** *"todo documento, anexo,
pdf, imagem que for ser salvo dos cadastros dos colaboradores, lembrar de salvar
no Google Drive."* O Arquivo do ERP já guarda no Drive, privado, com a
credencial `GOOGLE_CREDENTIALS_BASE64`. O que já está no Dropbox é passivo, e
trazer é trabalho à parte.

## Os sistemas de fora que esses cenários tocam

- **MobPonto** — `https://www.mobponto.com.br/ponto/api/endpoint.php`, com
  autenticação básica: cadastra a pessoa, manda a foto e inativa no
  desligamento. Quando a API não resolve, manda e-mail ao DP para fazer à mão.
  **Pergunta aberta:** quem faz isso quando o Pipefy morrer.
- **Z-API (WhatsApp)** — avisa o requerente e o DP.
- **Telegram** — `https://aplicacoes.bwsconstrucoes.com.br/telegram/enviar`, que
  é o nosso. Se falhar, manda e-mail avisando da falha.
- **Dropbox** — ver acima.
