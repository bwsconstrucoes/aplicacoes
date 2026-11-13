# PDF Processor — histórico

Memória da área. O chat não lembra de nada; o que vale é o que está aqui.
O que o módulo faz e o formato do que ele recebe e devolve estão no `README.md`.

## Pendente AGORA (atualizado em 13/11/2026, fim do dia)

**No ramo `claude/gallant-edison-27966k`, ainda NÃO publicado:** as melhorias da
seção "O que foi feito em 13/11/2026". Falta só o "pode" do dono para juntar na
linha principal. Não há migração de banco. Depois de publicar, conferir no log
do Render as primeiras compilações (buscar `/compilar`, `❌ Ignorado` e
`LENTA: POST /compilar`).

**Regra do dono, dita em 13/11/2026:** *"Não vamos mexer em nada que envolva
parâmetros no Make, pois precisa mexer em cenários."* Tudo o que exige mudar o
Make está fora até ele dizer o contrário — inclusive senha nas rotas, desligar
o texto e pôr o cenário em uma chamada por vez. E nada que mude o que o Make
JÁ recebe.

**Perguntas que ficaram sem resposta** (o dono não lembra; não travam nada do
que foi feito, mas travam os itens H e L da lista):

- o cenário manda os arquivos por `links` ou por `attachments`?
- algum passo depois do `/compilar` usa o campo `texto`?
- o cenário roda uma chamada por vez ou várias juntas?
- como é um link devolvido hoje (termina em `dl=0` ou `dl=1`)?

As quatro se respondem abrindo o cenário no Make, sem alterar nada nele.

## O que foi feito em 13/11/2026

Com o "pode" do dono (*"pode atualizar e fazer as melhorias"*), no código, sem
mudar nada do que o Make já recebe:

1. **Foto entra como veio** (`_foto_para_pdf`). Não é mais redesenhada. Medido:
   1 foto de 5 MB, de 1,9 s e +115 MB para 0,09 s e +41 MB; 5 fotos, de 9,4 s e
   +327 MB para 0,3 s e +147 MB; o PDF sai do tamanho da foto (antes, 3×). A
   página passou a ser A4 (em pé ou deitada). A rotação marcada pelo celular é
   respeitada — conferido contra o caminho antigo.
2. **PDF protegido que abre sem senha mantém as páginas** (`_reabrir_pelo_fitz`).
   Antes virava imagem de baixa resolução, 90× maior e sem texto. O mesmo
   caminho passou a salvar PDF com defeito leve, que antes também virava imagem.
3. **Página duplicada não acontece mais.** Falha ao ler o texto de uma página
   devolve texto vazio para ela, em vez de mandar o arquivo inteiro de novo
   como imagem.
4. **Esperas com prazo:** download com 120 s no total (`PRAZO_DOWNLOAD_S`) e 10 s
   para conectar; renovação do acesso ao Dropbox com 30 s.
5. **Campo novo `ignorados` na resposta:** o que ficou de fora e o motivo.
   `status`, `file`, `link` e `results` não mudaram.
6. **Testes:** `tests/test_pdf_processor.py`, 17 casos, os primeiros do módulo.
7. **A área entrou na tabela do `CLAUDE.md`** e na mensagem de abertura;
   registrado em `CONTEXTO.md` §9.

**O que ficou de fora, e por quê:**

- **PDF vazio continua sendo enviado** quando nada é aproveitado, e a resposta
  continua "ok". Deixar de enviar mudaria o que o Make recebe (`link` nulo). O
  `ignorados` já diz que nada entrou, para quando o Make puder olhar.
- **Limite de compilações simultâneas no servidor** (era o item F). Avaliado e
  NÃO feito: quem espera a vez continua ocupando uma linha de atendimento, e
  por mais tempo — pioraria justamente o que trava o resto do sistema. Só
  protegeria a memória, que hoje tem folga (4 GB) e ficou mais leve com o item
  1. O jeito certo de ter uma por vez é no Make.
- **Ler o texto com a outra biblioteca** (item H): o texto sairia um pouco
  diferente, e não se sabe se o Make procura trecho nele.
- **Teto para arquivo embutido** (item N): recusaria arquivo que hoje passa.
- **Link `dl=0`/`dl=1`** (item L): muda o que o Make recebe.
- **`/pdf2texto`**: não foi tocado nem ganhou teste.
- **O `app.py` antigo na raiz**: não foi tocado (é de fora da área).

**O que não foi verificado:** nada disto rodou contra o Dropbox, o Google Drive
ou o Make de verdade — só contra arquivos fabricados aqui. As fotos de teste
são JPG e PNG comuns; foto em outro formato (HEIC de iPhone, por exemplo) não
abria antes e continua não abrindo.

## Como o `/compilar` trabalha, passo a passo

1. Recebe o pedido inteiro na memória (os arquivos embutidos vêm dentro dele).
2. Para cada arquivo, **um de cada vez**: baixa (se for link), tenta abrir como
   PDF, copia as páginas para o PDF final e, se `incluir_texto` não foi
   desligado, lê o texto de cada página.
3. Foto JPG/PNG vira uma página A4 com a foto como veio. PDF protegido ou que
   o PyPDF2 não abre é regravado pelo `fitz`, com as páginas originais. Só o
   que sobra cai no **último recurso**: desenhar cada página como imagem.
   (Até 13/11/2026, foto e PDF protegido caíam direto no último recurso — era
   de onde vinham o tempo e a memória.)
4. Monta o PDF final inteiro na memória.
5. Envia ao Dropbox em pelo menos três idas: pergunta se o nome já existe (uma
   ida por nome ocupado), envia, cria o link público.
6. Responde com o link e os textos.

A linha de atendimento fica presa do passo 1 ao 6.

## Medições (13/11/2026, ANTES das melhorias)

Feitas neste repositório, fora da produção, com arquivos fabricados (notas de
texto, páginas escaneadas de ~5 MB, fotos de ~5 MB), sem enviar ao Dropbox.
Servem para comparar caminhos, não para prever o tempo exato no Render.

| Cenário | Tempo | Memória a mais | Resposta |
|---|---|---|---|
| 3 notas de 3 páginas | 0,1 s | 1 MB | 54 KB |
| 1 PDF de 40 páginas, com texto | 0,4 s | 10 MB | 239 KB |
| o mesmo, sem texto | 0,14 s | 8 MB | 0,1 KB |
| 5 PDFs de 40 páginas, com texto | 1,8 s | 45 MB | 1,2 MB |
| os mesmos, sem texto | 0,5 s | 35 MB | 0,3 KB |
| 1 PDF escaneado de 24 MB | 0,2 s | 108 MB | — |
| 2 PDFs escaneados (48 MB) | 0,5 s | 270 MB | — |
| 1 foto JPG de 5 MB | 1,9 s | 115 MB | — |
| 5 fotos JPG | 9,4 s | 327 MB | — |

O que as medições dizem:

- **PDF comum é barato.** O módulo não é lento por natureza.
- **Ler o texto triplica o tempo** e é o que faz a resposta ficar grande.
- **Foto é o caso caro:** ~2 s e ~100 MB por foto, e o PDF sai 3× maior que a
  foto (4,9 MB → 14,7 MB), numa página de 65 × 93 cm.
- **Memória anda junto com o tamanho dos arquivos, multiplicada por 4 a 5.**
  Dois escaneados de 24 MB custam 270 MB. Arquivo embutido não tem teto.
- **PDF protegido** (abre sem senha, tem trava de edição): 50 KB viram 4,5 MB de
  imagem em baixa resolução, sem texto.
- **O que não foi medido:** a internet. Baixar N links é N esperas em fila; o
  Dropbox são 3 idas ou mais. Em produção isso deve ser a maior fatia.

Protótipo dos consertos (fora do módulo, só para ter o número):

| O quê | Hoje | Com o conserto |
|---|---|---|
| 1 foto JPG | 1,9 s, +115 MB, sai 14,7 MB | 0,03 s, +23 MB, sai 4,9 MB, página A4 |
| PDF protegido | 4,5 MB, sem texto | 50 KB, com texto, páginas originais |
| texto de 40 páginas | 0,29 s | 0,08 s (outra biblioteca — o texto sai quase igual, não idêntico) |

## Defeitos encontrados na leitura (3 e 4 corrigidos em 13/11/2026; 2 e 5 em parte; o resto segue)

1. **Nenhum arquivo aproveitado → PDF vazio enviado, resposta "ok"** com link
   para um arquivo sem páginas. O Make segue como se tivesse dado certo.
2. **Arquivo pulado não aparece na resposta**, só no log do Render.
3. **PDF protegido vira imagem ruim** (acima). Comprovante de banco costuma vir
   assim.
4. **Página duplicada, em caso raro:** se o PDF abre, as páginas são copiadas e
   SÓ DEPOIS a leitura do texto falha, o caminho alternativo cola as mesmas
   páginas de novo, como imagem. Visto no código, não reproduzido.
5. **Esperas sem prazo:** a renovação do acesso ao Dropbox não tem limite de
   tempo (o `baixabradesco`, que copiou este código, já pôs 30 s). O download
   tem 60 s **por trecho**, não no total — um servidor que manda devagar segura
   a linha por muito mais.
6. **Apagar depois de N segundos se perde** se o serviço reinicia nesse
   intervalo (publicação, renovação): o arquivo fica no Dropbox para sempre.
7. **Falha no envio vira erro 500 com página de erro**, não JSON.
8. **Sem senha** nas três rotas.
9. **Link do Dropbox:** o código troca `?dl=0` por `?dl=1` (baixar direto). Os
   links atuais do Dropbox terminam em `&dl=0`, que essa troca não pega — o
   `baixabradesco` e a `emissaonf` já tratam os dois. Suspeita: o link sai como
   "abrir no site", não "baixar". **Não confirmado** — precisa de um link real.
   E cuidado: se o Make já se acomodou ao link como ele é, corrigir muda o que
   ele recebe.

## Melhorias possíveis (apresentadas ao dono em 13/11/2026)

**Feitas:** A, B, C, D e a metade de I (o campo `ignorados`). **Descartada:** F.
**Fora por exigir o Make ou mudar o que ele recebe:** E, G, L, M, P e a outra
metade de I. **Em aberto, decisão do dono:** H, J, K, N, O.

Ordem: da que mais rende com menos risco para a que menos.

| | Melhoria | Ganho esperado | Risco |
|---|---|---|---|
| A | Testes que travam o formato da resposta, antes de qualquer outra | Rede de proteção: toda mudança seguinte é conferida contra o que o Make recebe | Nenhum |
| B | Foto entra no PDF como ela é, sem redesenhar | ~60× mais rápido por foto, 1/5 da memória, arquivo 1/3 do tamanho | Baixo. A página passa a ter tamanho A4 (hoje é cartaz) — muda a aparência, para melhor |
| C | PDF protegido mantém as páginas originais | Legível, com texto, ~90× menor | Baixo. O PDF final sai sem a trava de edição do original |
| D | Prazo em toda espera de fora (Dropbox, Google, download no total) | Uma compilação presa deixa de segurar a linha por tempo indefinido | Baixo. Arquivo muito lento de baixar passa a ser pulado em vez de esperado |
| E | No Make: *Sequential processing* no cenário (uma compilação por vez) | Compilação nunca ocupa mais de 1 das 4 linhas. Sem código. Foi o que resolveu o `processarnovasp` em 12/11/2026 | Baixo. Em rajada, as compilações esperam a vez |
| F | Trava no servidor: no máximo 2 compilações juntas, a 3ª espera | O mesmo de E, garantido mesmo se outro cenário chamar | Baixo-médio. Quem espera continua ocupando linha; precisa de teto de espera |
| G | No Make: mandar `incluir_texto: false` se o `texto` não é usado | Compilação ~3× mais rápida, resposta de 1 MB vira 0,3 KB. Sem código | Nenhum, SE o Make não usa o texto |
| H | Ler o texto com a outra biblioteca | ~3,5× mais rápido na leitura | Médio. O texto sai com espaços e quebras um pouco diferentes; se o Make procura trecho nele, pode quebrar |
| I | Dizer na resposta o que foi pulado (campo novo `ignorados`) e não enviar PDF vazio | O Make passa a poder avisar "faltou o anexo X" | Baixo para o campo novo. Não enviar o vazio muda a resposta nesse caso (`link` nulo) — decisão do dono |
| J | Menos idas ao Dropbox (deixar o Dropbox escolher o nome livre; reaproveitar a conexão) | ~1 ida a menos por compilação, mais quando o nome já existe | Baixo. O nome repetido passaria de `nome(1).pdf` para `nome (1).pdf` |
| K | Baixar os links em paralelo | Com N links, o tempo de download cai para perto do do mais lento | Médio. Mais memória ao mesmo tempo. Só vale se o log mostrar que o download é o gargalo |
| L | Conferir e corrigir o link `dl=0`/`dl=1` | Link que baixa direto | Médio: muda o que o Make recebe |
| M | Senha (secret) nas rotas | Fecha a porta para estranhos | Médio: exige mudar o Make no mesmo momento, senão ele para |
| N | Teto de tamanho também para arquivo embutido | Protege a memória | Baixo-médio: arquivo legítimo acima do teto passa a ser recusado |
| O | Apagar o `app.py` antigo da raiz | Some a cópia sem as correções de memória | Baixo, mas atravessa áreas (`CONTEXTO.md` §10) |
| P | Compilar em segundo plano (responde na hora, o Make busca depois) | A linha é liberada em milissegundos | Alto: muda o contrato com o Make e precisa de lugar para guardar o trabalho. Não recomendado agora |

## O que veio antes (resumo; o detalhe está em `CONTEXTO.md` §9)

- **13–14/07/2026 — falta de memória no Render.** Este módulo era uma das
  causas: baixava o arquivo inteiro para a memória, não fechava os documentos
  do `fitz` e segurava as imagens. Corrigido no dia: download em pedaços com
  teto de 50 MB, fechamento garantido, e a opção `incluir_texto`.
- **15/10/2026 — nova queda por memória.** O `/compilar` estava entre o que
  rodava nos 2 minutos antes. O plano do Render subiu para 4 GB. Ficou em
  aberto, para este chat: medir o pico de cada tarefa pesada e não deixar duas
  rodarem juntas — é o que as melhorias E/F e N atacam.
- **31/10/2026 — `app/lentidao.py`.** Passou a existir o registro `LENTA` e
  `LINHAS OCUPADAS` no log. É por ele que se confirma, em produção, quanto o
  `/compilar` demora de verdade.
- **13/11/2026 — a área ganhou memória** (este arquivo e o `README.md`) e as
  primeiras melhorias (seção "O que foi feito em 13/11/2026").
