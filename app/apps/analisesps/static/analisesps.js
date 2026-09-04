// ---------------------------------------------------------------------------
// A BARRA DE AÇÕES — uma só por tela, para o que estiver marcado.
//
// Antes cada tabela do Lote tinha a sua barra: com seis grupos, seis barras de
// botões iguais, e cada uma só enxergava o próprio grupo. Agora a marcação é
// da TELA — marcar em grupos diferentes e agendar tudo de uma vez é uma ação
// só. É como era no Streamlit, que tinha uma barra fixa no alto valendo para a
// seleção inteira.
//
// Sem biblioteca: são cem linhas. Cada biblioteca nova é peso que a instância
// de 2 GB divide com quinze outros módulos.
// ---------------------------------------------------------------------------
(function () {
  const barra = document.getElementById("barra-acoes");
  if (!barra) return;

  const marcas = () => Array.from(document.querySelectorAll("input.marca"));
  const marcadas = () => marcas().filter(c => c.checked);

  const moeda = v => v.toLocaleString("pt-BR",
      {style: "currency", currency: "BRL"});

  function atualizar() {
    const sel = marcadas();
    const total = sel.reduce(
        (soma, c) => soma + (parseFloat(c.dataset.valor || "0") || 0), 0);

    const quantos = document.getElementById("ba-quantos");
    const valor = document.getElementById("ba-valor");
    if (quantos) {
      quantos.textContent = sel.length === 0 ? "Nenhuma SP marcada"
          : sel.length + (sel.length === 1 ? " SP marcada" : " SPs marcadas");
    }
    if (valor) valor.textContent = moeda(total);
    barra.classList.toggle("tem-selecao", sel.length > 0);

    marcas().forEach(c => c.closest("tr").classList.toggle("marcada", c.checked));

    // "Marcar todas" de cada grupo reflete o estado real do grupo dela.
    document.querySelectorAll("input.marcar-todas").forEach(t => {
      const grupo = t.dataset.grupo;
      const doGrupo = grupo === undefined ? marcas()
          : marcas().filter(c => c.dataset.grupo === grupo);
      const marcadasNo = doGrupo.filter(c => c.checked).length;
      t.checked = doGrupo.length > 0 && marcadasNo === doGrupo.length;
      t.indeterminate = marcadasNo > 0 && marcadasNo < doGrupo.length;
    });

    // Botões que só fazem sentido com algo marcado.
    barra.querySelectorAll("[data-precisa-selecao]").forEach(b => {
      b.disabled = sel.length === 0;
    });
  }

  document.querySelectorAll("input.marcar-todas").forEach(t => {
    t.addEventListener("change", () => {
      const grupo = t.dataset.grupo;
      const alvo = grupo === undefined ? marcas()
          : marcas().filter(c => c.dataset.grupo === grupo);
      alvo.forEach(c => { c.checked = t.checked; });
      atualizar();
    });
  });
  marcas().forEach(c => c.addEventListener("change", atualizar));

  function idsMarcados(minimo) {
    const ids = marcadas().map(c => c.value);
    if (ids.length < (minimo || 1)) {
      alert("Marque ao menos uma SP.");
      return null;
    }
    return ids;
  }

  // --- Alterar coluna (status de pagamento e agendamento) ------------------
  barra.querySelectorAll("button[data-coluna]").forEach(botao => {
    botao.addEventListener("click", async () => {
      const ids = idsMarcados();
      if (!ids) return;
      const rotulo = botao.dataset.rotulo || botao.textContent.trim();
      const valor = botao.dataset.valor || "";
      const efeito = valor === ""
          ? `APAGAR o agendamento de ${ids.length} SP(s)`
          : `${rotulo}: ${ids.length} SP(s)`;
      if (!confirm(`${efeito}.\n\nA alteração vai para a planilha. Confirma?`)) return;

      botao.disabled = true;
      try {
        const r = await fetch(barra.dataset.urlAlterar, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({ids: ids, coluna: botao.dataset.coluna,
                                valor: valor, acao: rotulo})
        });
        const d = await r.json();
        if (!d.ok) { alert("Não deu certo: " + (d.erro || "erro desconhecido")); return; }
        if (d.aviso) alert("Alterado aqui, mas o envio para a planilha ficou na fila:\n" + d.aviso);
        location.reload();
      } catch (e) {
        alert("Falhou a comunicação com o servidor: " + e);
      } finally {
        botao.disabled = false;
      }
    });
  });

  // --- QR Pix / código de barras das marcadas ------------------------------
  const btnCodigos = document.getElementById("ba-codigos");
  if (btnCodigos) btnCodigos.addEventListener("click", () => {
    const ids = idsMarcados();
    if (!ids) return;
    if (ids.length > 50) { alert("São no máximo 50 SPs por vez."); return; }
    const volta = barra.dataset.origem || "";
    location.href = barra.dataset.urlCodigos + "?"
        + ids.map(i => "id=" + encodeURIComponent(i)).join("&")
        + (volta ? "&origem=" + encodeURIComponent(volta) : "");
  });

  // --- Mandar as marcadas para o lote --------------------------------------
  const btnLote = document.getElementById("ba-enviar-lote");
  if (btnLote) btnLote.addEventListener("click", () => {
    const ids = idsMarcados();
    if (!ids) return;
    const form = document.getElementById("form-enviar-lote");
    form.querySelector("input[name=ids]").value = ids.join(",");
    form.submit();
  });

  // --- Validar as marcadas -------------------------------------------------
  //
  // Grava Validacao = "Sim". E a mesma escrita de sempre (banco, fila, log,
  // planilha), so que na coluna AH. O que muda e o significado: validar e o
  // que destrava o agendamento, entao pede senha PROPRIA — se a de Operador
  // servisse, quem agenda seria o mesmo que autoriza a agendar.
  const btnValidar = document.getElementById("ba-validar");
  if (btnValidar) btnValidar.addEventListener("click", async () => {
    const ids = idsMarcados();
    if (!ids) return;
    const senha = prompt(`Validar ${ids.length} SP(s) — marca Validação = "Sim".`
                         + `\n\nSenha de validação:`);
    if (senha === null) return;
    btnValidar.disabled = true;
    try {
      const r = await fetch(barra.dataset.urlValidar, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ids: ids, senha: senha})
      });
      const d = await r.json();
      if (!d.ok) { alert("Não deu certo: " + (d.erro || "erro desconhecido")); return; }
      location.reload();
    } catch (e) { alert("Falhou a comunicação com o servidor: " + e); }
    finally { btnValidar.disabled = false; }
  });

  // --- Tirar do lote o que esta marcado ------------------------------------
  //
  // Mexe SO na lista do lote: nao altera status, nao vai para a planilha.
  // A confirmacao diz isso, porque "remover" numa tela de pagamentos assusta.
  const btnRemover = document.getElementById("ba-remover-lote");
  if (btnRemover) btnRemover.addEventListener("click", () => {
    const ids = idsMarcados();
    if (!ids) return;
    if (!confirm(`Tirar ${ids.length} SP(s) do lote.\n\nIsto mexe só na sua `
                 + `lista — não altera nada na planilha nem no Pipefy. `
                 + `Confirma?`)) return;
    const form = document.getElementById("form-remover-lote");
    form.querySelector("input[name=ids]").value = ids.join(",");
    form.submit();
  });

  // --- Abrir no Pipefy os cards das marcadas -------------------------------
  //
  // Uma aba por card. O navegador bloqueia isso por padrão quando não parte de
  // um clique — parte, mas o aviso fica de qualquer forma, porque o bloqueio
  // silencioso é o que faz alguém achar que o botão não funciona.
  const btnCards = document.getElementById("ba-cards");
  if (btnCards) btnCards.addEventListener("click", () => {
    const links = marcadas()
        .map(c => c.dataset.card)
        .filter(u => u && u.startsWith("http"));
    if (!links.length) {
      alert("Nenhuma das SPs marcadas tem link de card do Pipefy.");
      return;
    }
    if (links.length > 15 &&
        !confirm(`Isso vai abrir ${links.length} abas. Continua?`)) return;
    let bloqueada = false;
    links.forEach(u => { if (!window.open(u, "_blank", "noopener")) bloqueada = true; });
    if (bloqueada) {
      alert("O navegador bloqueou as abas. Libere as janelas pop-up para este "
            + "site e tente de novo.");
    }
  });

  atualizar();
})();


// ---------------------------------------------------------------------------
// OS BOTÕES DA FICHA — valem para a página inteira e para o modal.
//
// Exposto em window porque o modal carrega a ficha DEPOIS que esta página já
// rodou: quem monta o conteúdo precisa avisar aqui para os botões passarem a
// funcionar. Sem isso, a ficha no modal abriria bonita e inerte.
// ---------------------------------------------------------------------------
window.ligarFicha = function (raiz) {
  // Um clique no campo do codigo seleciona tudo — quem esta pagando copia e
  // cola sem mirar. Vale tambem para a ficha aberta no modal, onde estes
  // campos nascem depois que a pagina ja rodou.
  (raiz || document).querySelectorAll(".copiavel").forEach(campo => {
    if (campo.dataset.ligada) return;
    campo.dataset.ligada = "1";
    campo.addEventListener("focus", () => campo.select());
    campo.addEventListener("click", () => campo.select());
  });

  const caixa = (raiz || document).querySelector(".ficha-acoes");
  if (!caixa || caixa.dataset.ligada) return;
  caixa.dataset.ligada = "1";

  const sp = caixa.dataset.sp;
  const url = caixa.dataset.urlAlterar;

  async function mandar(mudancas, rotulo) {
    for (const [coluna, valor] of mudancas) {
      const r = await fetch(url, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ids: [sp], coluna: coluna, valor: valor,
                              acao: rotulo})
      });
      const d = await r.json();
      if (!d.ok) { alert("Não deu certo: " + (d.erro || "erro desconhecido")); return false; }
    }
    return true;
  }

  caixa.querySelectorAll("button[data-coluna]").forEach(botao => {
    botao.addEventListener("click", async () => {
      const rotulo = botao.textContent.trim();
      const valor = botao.dataset.valor;
      // Trava da Validacao, como no Streamlit. Antes o botao vinha
      // `disabled`: nao gravava nada, mas tambem nao dizia nada — quem nao
      // leu o aviso logo acima achava que o botao estava quebrado.
      if (botao.dataset.bloqueado) {
        const validarAgora = caixa.querySelector("#ficha-validar");
        const querValidar = confirm(
          `Não dá para "${rotulo}" nesta SP: a coluna Validação precisa `
          + `estar como "Sim".\n\nQuer validar a SP ${sp} agora?`);
        if (querValidar && validarAgora) validarAgora.click();
        return;
      }
      const efeito = botao.dataset.coluna === "agendado" && valor === "Desagendar"
          ? `Apagar o agendamento da SP ${sp}`
          : `${rotulo} na SP ${sp}`;
      if (!confirm(efeito + ".\n\nIsto escreve na planilha SPsBD. O Pipefy NÃO "
                   + "é alterado. Confirma?")) return;
      botao.disabled = true;
      try {
        if (await mandar([[botao.dataset.coluna, valor]], rotulo)) location.reload();
      } catch (e) { alert("Falhou a comunicação com o servidor: " + e); }
      finally { botao.disabled = false; }
    });
  });

  const validar = caixa.querySelector("#ficha-validar");
  if (validar) validar.addEventListener("click", async () => {
    const senha = prompt(`Validar a SP ${sp} — marca Validação = "Sim" e `
                         + `destrava o agendamento.\n\nSenha de validação:`);
    if (senha === null) return;
    validar.disabled = true;
    try {
      const r = await fetch(caixa.dataset.urlValidar, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ids: [sp], senha: senha})
      });
      const d = await r.json();
      if (!d.ok) { alert("Não deu certo: " + (d.erro || "erro desconhecido")); return; }
      location.reload();
    } catch (e) { alert("Falhou a comunicação com o servidor: " + e); }
    finally { validar.disabled = false; }
  });

  // O botao de remover risco vive DENTRO do aviso de risco, e nao na caixa de
  // acoes — por isso e procurado no documento (ou no modal), nao na caixa.
  const semRisco = (raiz || document).querySelector("#ficha-sem-risco");
  if (semRisco) semRisco.addEventListener("click", async () => {
    if (!confirm(`Marcar a SP ${sp} como REVISADA — ela sai da lista de risco `
                 + `de duplicidade.\n\nFica registrado que foi você quem `
                 + `revisou. Confirma?`)) return;
    semRisco.disabled = true;
    try {
      const r = await fetch(caixa.dataset.urlSemRisco, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ids: [sp]})
      });
      const d = await r.json();
      if (!d.ok) { alert("Não deu certo: " + (d.erro || "erro desconhecido")); return; }
      location.reload();
    } catch (e) { alert("Falhou a comunicação com o servidor: " + e); }
    finally { semRisco.disabled = false; }
  });

  // "Limpar Pgto": as duas colunas de uma vez, como no Streamlit — Status Pgt
  // volta para "Pagar" e o Agendado fica vazio.
  const limpar = caixa.querySelector("#ficha-limpar");
  if (limpar) limpar.addEventListener("click", async () => {
    if (!confirm(`Limpar o pagamento da SP ${sp}: Status Pgt volta para "Pagar" `
                 + `e o Agendado fica vazio.\n\nIsto escreve na planilha SPsBD. `
                 + `Confirma?`)) return;
    limpar.disabled = true;
    try {
      const ok = await mandar([["status_pgt", "Pagar"], ["agendado", "Desagendar"]],
                              "Ficha: Limpar Pgto");
      if (ok) location.reload();
    } catch (e) { alert("Falhou a comunicação com o servidor: " + e); }
    finally { limpar.disabled = false; }
  });
};

// A ficha aberta como página inteira liga na hora.
document.addEventListener("DOMContentLoaded", () => window.ligarFicha(document));


// ---------------------------------------------------------------------------
// A BUSCA POR ATUALIZACOES DE 90 EM 90 SEGUNDOS
//
// O Streamlit tinha "Auto-atualizar (90s)", ligado por padrao. A conversao
// deixou de fora, e com isso a base so se atualizava quando alguem apertava o
// botao em Configuracoes — o agendador externo que deveria chamar a
// sincronizacao nao da sinal de ter sido configurado.
//
// Aqui a tela aberta faz duas coisas a cada 90 s: pede ao servidor que
// DISPARE a sincronizacao se ela estiver velha, e pergunta se a base mudou.
//
// E NAO RECARREGA SOZINHA COM SPs MARCADAS. Recarregar por baixo de quem
// acabou de marcar vinte linhas apagaria a selecao — e isso e pior do que ver
// um numero com dois minutos de idade. Nesse caso aparece um aviso discreto e
// quem decide e a pessoa.
// ---------------------------------------------------------------------------
(function () {
  const marca = document.getElementById("frescor");
  if (!marca) return;

  const CADA = 90000;
  const url = marca.dataset.url;
  let carimboInicial = marca.dataset.carimbo || "";
  let avisando = false;

  function temSelecao() {
    return document.querySelectorAll("input.marca:checked").length > 0;
  }

  function temModalAberto() {
    const modal = document.getElementById("ficha-modal");
    return !!(modal && modal.open);
  }

  function avisar() {
    if (avisando) return;
    avisando = true;
    const barra = document.createElement("div");
    barra.className = "aviso-frescor";
    barra.innerHTML =
      '<span>A base foi atualizada desde que você abriu esta tela.</span>' +
      '<button class="btn" type="button">Ver o que mudou</button>';
    barra.querySelector("button").addEventListener(
      "click", () => location.reload());
    document.body.appendChild(barra);
  }

  async function bater() {
    try {
      const r = await fetch(url, {headers: {"Accept": "application/json"}});
      if (!r.ok) return;                 // sessao caiu, rede oscilou: cala
      const d = await r.json();
      if (!d.carimbo) return;
      if (!carimboInicial) { carimboInicial = d.carimbo; return; }
      if (d.carimbo === carimboInicial) return;

      // Mudou. Se ninguem esta no meio de nada, recarrega; senao, avisa.
      if (temSelecao() || temModalAberto()) avisar();
      else location.reload();
    } catch (e) {
      // De fundo: um erro aqui nao pode aparecer na cara de quem so olhava.
    }
  }

  setInterval(bater, CADA);
})();
