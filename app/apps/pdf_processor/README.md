# PDF Processor

Junta vários arquivos (PDF e foto) num PDF só e devolve o link dele. É chamado
pelo **Make**, não por gente: não tem tela, não tem banco, não guarda nada entre
uma chamada e outra.

O módulo inteiro mora em `__init__.py` (não segue o padrão `routes.py` das outras
áreas — ver `CONTEXTO.md` §2). É registrado **sem prefixo** no `app/main.py`: as
rotas ficam na raiz do endereço (`pdf.bwsconstrucoes.com.br/compilar`).

O estado do trabalho, as medições e as decisões estão no `HISTORICO.md`.

## As três rotas

| Método | Rota | O que faz |
|---|---|---|
| POST | `/compilar` | Junta os arquivos recebidos num PDF, envia ao Dropbox (ou Google Drive) e devolve o link. |
| POST | `/pdf2texto` | Devolve o texto de cada página de cada PDF; opcionalmente salva cada página como um PDF separado no Dropbox. |
| GET | `/token-status` | Confere se a conexão com o Dropbox está de pé (devolve o nome da conta). |

⚠️ **Nenhuma das três pede senha (secret).** Quem souber o endereço consegue
usar. Está na lista de melhorias do `HISTORICO.md`; fechar exige mexer no Make
junto.

## `/compilar` — o que entra

Corpo em JSON. Tudo é opcional; os padrões estão na última coluna.

| Campo | Para quê | Padrão |
|---|---|---|
| `links` | Lista de endereços de arquivos para baixar. | `[]` |
| `attachments` | Lista de arquivos embutidos: `{"filename": ..., "base64": ...}` ou `{"filename": ..., "data": <hexadecimal>}`. | `[]` |
| `nome_arquivo` | Nome do PDF final (ganha `.pdf` se não tiver). | `compilado.pdf` |
| `destino` | `dropbox` ou `googledrive`. | `dropbox` |
| `pasta` | Pasta do Dropbox, ou o ID da pasta do Drive. | `/pdf-compilados` |
| `salvar` | `false` = só monta e devolve o texto, sem enviar a lugar nenhum. | `true` |
| `incluir_texto` | `false` = não extrai o texto das páginas (mais rápido, resposta pequena). | `true` |
| `deletar` + `auto_delete` | Apaga o arquivo do Dropbox depois de `auto_delete` segundos. Só vale para Dropbox. | `false` / `300` |

**Ordem das páginas no PDF final:** primeiro todos os `links`, na ordem
recebida; depois todos os `attachments`.

**Limites:** arquivo baixado por link tem teto de 50 MB (`MAX_DOWNLOAD_BYTES`) —
passou disso, é pulado. Arquivo embutido (`attachments`) **não tem teto**.

## `/compilar` — o que sai (é disto que o Make depende)

```json
{
  "status": "ok",
  "file": "compilado.pdf",
  "link": "https://www.dropbox.com/...",
  "results": [
    {"filename": "nota.pdf", "texto": ["texto da página 1", "texto da página 2"]}
  ]
}
```

O que o Make pode estar usando, e portanto **não pode mudar sem conferir o
cenário lá**:

- **Os quatro nomes** `status`, `file`, `link`, `results`, e dentro de cada
  resultado `filename` e `texto`.
- **`texto` é uma lista, uma posição por página.** Vem vazia (`[]`) com
  `incluir_texto: false`, e com textos vazios (`""`) para foto e PDF escaneado.
- **`results` só traz o que deu certo.** Arquivo pulado some da lista sem aviso
  — a posição dos seguintes anda. Se o Make pega "o 2º resultado", pega o
  arquivo errado quando o 1º falha.
- **`link`** é `null` quando `salvar` é `false`.
- **O código de resposta é sempre 200**, mesmo quando nenhum arquivo foi
  aproveitado (aí o PDF enviado sai vazio). Só vira erro 500 — com página de
  erro, não JSON — quando o envio ao Dropbox/Drive falha.
- **O conteúdo do texto**: se algum passo do Make procura um trecho dentro do
  `texto` (número de nota, valor, CNPJ), trocar a forma de extrair muda espaços
  e quebras de linha, e a busca pode deixar de achar.

Mudança segura é **acrescentar** campo. Tirar, renomear ou mudar o tipo de um
campo existente só com o cenário do Make aberto ao lado.

## O que acontece com cada tipo de arquivo (medido em 13/11/2026)

| Entra | Sai |
|---|---|
| PDF comum | As mesmas páginas, intactas. Rápido. |
| PDF escaneado | As mesmas páginas, intactas; `texto` vazio. |
| Foto solta (JPG/PNG) | Vira página — mas é redesenhada: demora ~2 s por foto, o arquivo sai ~3× maior e a página fica com tamanho de cartaz. |
| PDF protegido (abre sem senha, mas tem trava de edição — comum em comprovante de banco) | As páginas viram **imagem de baixa resolução**: perde o texto, fica ~90× maior e pior de ler. |
| PDF que pede senha para abrir | Pulado. |
| Qualquer outra coisa (Word, Excel, arquivo corrompido) | Pulado. |

"Pulado" quer dizer: uma linha `❌ Ignorado (...)` no log do Render e nada na
resposta.

## `/pdf2texto`

Recebe `attachments` (com `url`, `base64`, `data` ou `hex`) e/ou `links`. Só
aceita PDF de verdade. Devolve:

```json
{"status": "ok", "results": [{"filename": "...", "paginas": [{"numero": 1, "texto": "...", "link": "..."}]}]}
```

O `link` de cada página só aparece com `salvar: true` — e aí cada página é um
envio separado ao Dropbox (um PDF de 40 páginas são 40 envios, um atrás do
outro). Sem arquivo nenhum no pedido, responde 400 com `{"erro": ...}`.

## Do que depende

- **Dropbox:** `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`.
- **Google Drive** (só com `destino: googledrive`): `GOOGLE_CREDENTIALS_BASE64`.
- **Bibliotecas:** PyPDF2 (juntar páginas e ler texto) e PyMuPDF/`fitz` (o
  caminho das fotos e dos PDFs que o PyPDF2 não abre).

## Quem copia este módulo

- `app/apps/baixabradesco/storage.py` replica o envio ao Dropbox daqui (já com
  o prazo de 30 s na renovação do acesso, que aqui falta).
- `app.py`, na raiz do repositório, é a versão **antiga** deste módulo, de antes
  do monorepo. Não é o que roda em produção (`Procfile` aponta para
  `app.main:app`). Ficou sem as correções de memória de julho de 2026.

## Regras ao mexer

- Memória: `CONTEXTO.md` §3.7 (download com teto, fechar documento do `fitz`,
  soltar o que é grande ao fim). A falta de memória de julho de 2026 começou
  aqui (`CONTEXTO.md` §9).
- O servidor tem 1 processo e 4 linhas de atendimento. Uma compilação segura uma
  linha do começo ao fim, incluindo a espera pelo Dropbox. Quatro juntas param
  o ERP, o ponto e todo o resto (`app/lentidao.py`).
- **Não há teste automatizado do módulo** (em 13/11/2026). Antes de mudar o
  comportamento, escrever o teste que trava o formato da resposta.
