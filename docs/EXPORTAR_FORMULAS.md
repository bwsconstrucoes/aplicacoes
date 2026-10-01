# Como me entregar as fórmulas das planilhas — TODAS de uma vez

Escrito em 26/09/2026. O dono cobrou, e está certo:

> *"Você precisa ler as fórmulas das planilhas. Nelas foi criado todo o
> regramento, do contrário você vai criar algo errado."*
>
> *"Mas eu preciso exportar as fórmulas logo de todas as planilhas, para não ser
> de uma em uma. E ainda quero exportar de mais uma, que será do próximo
> trabalho."*

O acesso que eu tenho ao Drive devolve **valores**, não fórmulas. A saída é um
script que junta **só as fórmulas** e as escreve em documentos que eu consigo ler.

⚠️ **Por que não é só "baixar a planilha e mandar":** em `.xlsx` as fórmulas vêm —
mas vêm junto **nome, CPF, salário e endereço de ~500 pessoas**. Eu preciso da
regra, não dos dados. Este script manda só a regra.

---

## Passo a passo (uma vez só, serve para todas)

1. Abra **script.google.com** e clique em **Novo projeto**.
2. Apague o que estiver lá e **cole o código inteiro** que está mais abaixo.
3. No alto do código, na lista `PLANILHAS`, **cole o endereço de cada planilha**
   (o link da barra de endereços serve; o ID também). Já deixei as oito que
   importam — **acrescente a do próximo trabalho na mesma lista**.
4. Salve (o nome do projeto pode ser qualquer um, ex.: "Exportar Fórmulas BWS").
5. Escolha a função **`exportarTodas`** e clique em **Executar**. Na primeira vez
   o Google pede permissão: autorize.
6. Ao terminar, ele cria na sua pasta principal do Drive a pasta
   **`FÓRMULAS BWS`**, com **um documento por planilha**. Me avise que está pronto.

⚠️ **Se der "tempo excedido"** (o Google corta em 6 minutos), **rode
`exportarTodas` de novo**. Ele lembra o que já terminou e continua de onde parou.
Rode até aparecer a mensagem de que acabou.

Para refazer tudo do zero depois de mudar a lista, rode **`recomecarDoZero`** e
depois `exportarTodas`.

---

## O código

```javascript
/**
 * Junta num documento SÓ as fórmulas de cada planilha — nenhum dado de pessoa.
 *
 * COLE AQUI o link (ou o ID) de cada planilha. Para acrescentar uma nova, basta
 * pôr o link numa linha nova, com vírgula no fim da linha anterior.
 */
const PLANILHAS = [
  "https://docs.google.com/spreadsheets/d/10l90ZS4N_E98mgsVDdqX6x3TsTSrXrgFXqsG3N_BdQw",  // Folha de Pagamento - Fortes  (a mais importante)
  "https://docs.google.com/spreadsheets/d/1Qgf8XievSFo4SG9R7V90ILPgSIG9UQ1r5G0SASgOPhc",  // Relatório de Análise - Quinzena CTPS
  "https://docs.google.com/spreadsheets/d/1cr9oFjlh8iZ910eBSeCKKZygx8RvXKDyww7BzzxmNqA",  // Mobponto - Presença e Cadastros
  "https://docs.google.com/spreadsheets/d/1Q2Qz3Uy1SiwBNWOyZMxHLTpl_ds_n4VR_mj1llRv2to",  // Mobponto - Relatório Geral Mensal
  "https://docs.google.com/spreadsheets/d/1fqi4QUOVGUd1_4Gg4vK5qP_IMOSgFaw8DD9MDgmM3vo",  // Registro de Colaboradores
  "https://docs.google.com/spreadsheets/d/1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA",  // Registro de SPs (tem a aba C. Diários)
  "https://docs.google.com/spreadsheets/d/1Q39sdTbZ4edNthTU3HsCc8ahkBLWfqOcffbBXp3_RI8",  // Diaristas, Extras e GM (abas CTPS, Diaristas, GM, Cesta, Alimentação, Transporte)
  "https://docs.google.com/spreadsheets/d/1LoTJtYKHpSuxnvr03tBvxjpLK6c2Wvuyo4YbVk3IYEk",  // Planilha de Análise (a que vai anexada no card da BeeVale)
  // ↓ ACRESCENTE AQUI a planilha do próximo trabalho:
  // "cole o link aqui",
];

const PASTA = "FÓRMULAS BWS";
const LINHAS_LIDAS = 60;        // as primeiras bastam: a regra mora no alto
const MINUTOS_LIMITE = 5;       // para antes do corte do Google, em 6

function exportarTodas() {
  const inicio = Date.now();
  const memoria = PropertiesService.getScriptProperties();
  const prontas = JSON.parse(memoria.getProperty("prontas") || "[]");
  const pasta = pasta_();
  const feitasAgora = [];
  const falhas = [];

  for (const endereco of PLANILHAS) {
    const id = idDe_(endereco);
    if (!id) { falhas.push(endereco + " → não reconheci o link"); continue; }
    if (prontas.indexOf(id) >= 0) continue;              // já terminou antes

    if ((Date.now() - inicio) / 60000 > MINUTOS_LIMITE) {
      avisar_("Parei no tempo. Já terminei " + prontas.length + " planilha(s).\n\n" +
              "RODE 'exportarTodas' DE NOVO para continuar de onde parou.");
      return;
    }

    try {
      const ss = SpreadsheetApp.openById(id);
      const texto = formulasDaPlanilha_(ss);
      salvar_(pasta, "FÓRMULAS - " + ss.getName(), texto);
      prontas.push(id);
      feitasAgora.push(ss.getName());
      memoria.setProperty("prontas", JSON.stringify(prontas));
    } catch (e) {
      falhas.push(endereco + " → " + (e.message || e));
    }
  }

  let recado = "Acabou.\n\nDocumentos na pasta \"" + PASTA + "\" do seu Drive.\n";
  if (feitasAgora.length) recado += "\nFeitas agora:\n- " + feitasAgora.join("\n- ");
  if (falhas.length) recado += "\n\nNÃO deu:\n- " + falhas.join("\n- ");
  avisar_(recado);
}

/** Esquece o que já foi feito, para refazer tudo. */
function recomecarDoZero() {
  PropertiesService.getScriptProperties().deleteProperty("prontas");
  avisar_("Memória limpa. Agora rode 'exportarTodas'.");
}

// ---------------------------------------------------------------------------

function formulasDaPlanilha_(ss) {
  const partes = ["FÓRMULAS — " + ss.getName(),
                  ss.getUrl(),
                  "gerado em " + new Date().toLocaleString("pt-BR"),
                  "(só fórmulas; nenhum dado de pessoa foi copiado)", ""];

  ss.getSheets().forEach(aba => {
    const linhas = Math.min(aba.getLastRow(), LINHAS_LIDAS);
    const colunas = aba.getLastColumn();
    partes.push("========== ABA: " + aba.getName() +
                "   (" + aba.getLastRow() + " linhas x " + colunas + " colunas)" +
                (aba.isSheetHidden() ? "  [OCULTA]" : ""));
    if (!linhas || !colunas) { partes.push("(vazia)", ""); return; }

    const formulas = aba.getRange(1, 1, linhas, colunas).getFormulas();
    const cabecalho = aba.getRange(1, 1, Math.min(4, linhas), colunas).getValues();
    let achou = false;

    for (let c = 0; c < colunas; c++) {
      let rotulo = "";
      for (let r = 0; r < cabecalho.length; r++) {
        const v = String(cabecalho[r][c] || "").trim();
        if (v && v.length < 60) { rotulo = v; break; }
      }
      let primeira = "", onde = 0, comFormula = 0;
      const padroes = {};
      for (let r = 0; r < linhas; r++) {
        const f = formulas[r][c];
        if (!f) continue;
        if (!primeira) { primeira = f; onde = r + 1; }
        padroes[f.replace(/\d+/g, "#")] = r + 1;   // 'A2' e 'A3' = mesmo padrão
        comFormula++;
      }
      if (!primeira) continue;
      achou = true;
      const quantos = Object.keys(padroes).length;
      partes.push(letra_(c + 1) + "  [" + rotulo + "]  (linha " + onde + "; " +
                  comFormula + " com fórmula; " + quantos + " padrão(ões))");
      partes.push("    " + primeira);
      // Mais de um padrão na mesma coluna é onde mora a exceção que ninguém
      // lembra — então manda os outros também.
      if (quantos > 1) {
        const base = primeira.replace(/\d+/g, "#");
        const jaFoi = {};
        jaFoi[base] = true;
        for (let r = 0; r < linhas; r++) {
          const f = formulas[r][c];
          if (!f) continue;
          const chave = f.replace(/\d+/g, "#");
          if (jaFoi[chave]) continue;
          jaFoi[chave] = true;
          partes.push("    (outro padrão, linha " + (r + 1) + ") " + f);
        }
      }
    }
    if (!achou) partes.push("(nenhuma fórmula nas primeiras " + linhas + " linhas)");
    partes.push("");
  });
  return partes.join("\n");
}

function salvar_(pasta, nome, texto) {
  // Documento novo a cada vez, com a data no nome: assim nada é sobrescrito e dá
  // para comparar duas gerações se a planilha mudar.
  const marca = Utilities.formatDate(new Date(),
      Session.getScriptTimeZone() || "America/Fortaleza", "dd.MM.yyyy HH.mm");
  const doc = DocumentApp.create(nome + " (" + marca + ")");
  doc.getBody().setText(texto);
  doc.saveAndClose();
  DriveApp.getFileById(doc.getId()).moveTo(pasta);
}

function pasta_() {
  const achadas = DriveApp.getFoldersByName(PASTA);
  return achadas.hasNext() ? achadas.next() : DriveApp.createFolder(PASTA);
}

function idDe_(endereco) {
  const texto = String(endereco || "").trim();
  const m = texto.match(/\/d\/([a-zA-Z0-9-_]{20,})/);
  if (m) return m[1];
  return /^[a-zA-Z0-9-_]{20,}$/.test(texto) ? texto : "";
}

function letra_(numero) {
  let letras = "";
  while (numero > 0) {
    const resto = (numero - 1) % 26;
    letras = String.fromCharCode(65 + resto) + letras;
    numero = Math.floor((numero - 1) / 26);
  }
  return letras;
}

function avisar_(texto) {
  Logger.log(texto);
  try { MailApp.sendEmail(Session.getEffectiveUser().getEmail(),
                          "Exportar fórmulas BWS", texto); } catch (e) {}
}
```

---

## O que cada documento traz

Por aba, e por coluna que tenha fórmula:

```
========== ABA: Quinzena   (917 linhas x 40 colunas)
AH  [Vínculo]  (linha 2; 915 com fórmula; 1 padrão(ões))
    =ARRAYFORMULA(SEERRO(SE(A2:A="";"";SE(...
```

- **a letra da coluna** e o **rótulo** do cabeçalho, para eu saber do que se trata;
- **em que linha** a fórmula está e **quantas linhas** têm fórmula;
- **quantos padrões diferentes** existem na coluna — e, quando há mais de um, ele
  manda todos. É aí que mora a exceção que ninguém lembra de ter criado.

O aviso do fim chega **no seu e-mail**, porque script rodando sozinho não tem tela.

---

## Se o Apps Script não der

Aba por aba, numa célula: `=FORMULATEXT(Quinzena!AH2)`. Funciona, mas é uma
célula por vez — por isso o script.

---

## Por que isso vale o trabalho

As duas fórmulas recuperadas até agora **consertaram um erro cada**:

1. **A coluna AH da aba `Mobponto`** (ele colou em 26/09/2026): mostrou que CTPS ou
   DIÁRIA é decidido **por DIA**, não por pessoa. Eu ia jogar metade do dinheiro de
   quem foi admitido no meio da quinzena para o método de pagamento errado. Virou
   `app/apps/analisesps/folha_vinculo.py`.
2. **O cabeçalho da aba `C. Diários`** (achado por busca no Drive): a conta corrente
   de cada obra vinha sendo carregada **da coluna errada** — era o ID do Pipefy, não
   a conta. Ver `FOLHA_DE_PAGAMENTO.md` §7.1, D5.

Duas por duas. É por isso que, enquanto as fórmulas não chegarem, **tudo que
depende delas está em suposição** — e está dito assim no outro documento.
