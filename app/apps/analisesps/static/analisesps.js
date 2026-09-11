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
// ---------------------------------------------------------------------------
// A TELA VOLTA COMO ESTAVA: rolagem e caixinhas marcadas
//
// Pedido do dono: "e como se eu tivesse duas abas do navegador, alternando
// entre Solicitacoes e o Lote". A tela guardada ja volta na hora (ver o
// `Cache-Control` no `web.py`), mas voltava no TOPO e sem as marcacoes — e
// quem marcou vinte SPs, foi conferir uma no Lote e voltou, remarcava tudo.
//
// Fica na memoria da ABA (`sessionStorage`), nao no computador: fechou a aba,
// acabou. E a chave inclui o ENDERECO INTEIRO, com o filtro — mudou o filtro,
// as marcacoes de antes nao voltam, porque sao de outra lista.
//
// Meia hora de validade para nao ressuscitar uma selecao esquecida.
// ---------------------------------------------------------------------------
const LEMBRAR = {
  minutos: 30,
  chave(que) { return "analisesps:" + que + ":" + location.pathname + location.search; },
  ler(que) {
    try {
      const cru = sessionStorage.getItem(this.chave(que));
      if (!cru) return null;
      const guardado = JSON.parse(cru);
      if (Date.now() - (guardado.quando || 0) > this.minutos * 60000) return null;
      return guardado.valor;
    } catch (e) { return null; }
  },
  gravar(que, valor) {
    try {
      sessionStorage.setItem(this.chave(que),
                             JSON.stringify({valor: valor, quando: Date.now()}));
    } catch (e) { /* aba anonima, ou memoria cheia: seguir sem lembrar */ }
  },
  esquecer(que) {
    try { sessionStorage.removeItem(this.chave(que)); } catch (e) {}
  },
};

// ---------------------------------------------------------------------------
// O LINK DA ABA APONTA PARA A TELA COMO ELA ESTAVA
//
// ERA ISTO QUE FAZIA O CACHE NAO SERVIR PARA NADA, e so apareceu quando o
// dono disse "nao senti diferenca nenhuma". O link do menu aponta para
// `/analisesps/solicitacoes`, SEM filtro. O servidor recebe isso, ve que ha
// filtro guardado, e REDIRECIONA para `/analisesps/solicitacoes?...&f=1`.
//
// Ou seja: toda troca de aba ia ao servidor de qualquer jeito, e a copia
// guardada — que fica sob o endereco COM filtro — nunca era alcancada. O
// cache existia; o menu passava por fora dele.
//
// Aqui o link do menu e reescrito para o endereco que a pessoa realmente
// usou. Sem redirecionamento, e o navegador serve a tela guardada na hora.
//
// Fica no navegador, e nao no servidor, de proposito: montar esses links no
// servidor custaria uma consulta a mais em TODA tela, inclusive nas que nao
// tem filtro nenhum — pagar em todas para economizar em duas.
// ---------------------------------------------------------------------------
(function () {
  const TELAS_COM_FILTRO = ["/analisesps/solicitacoes", "/analisesps/relatorio"];
  const chave = caminho => "analisesps:endereco:" + caminho;

  try {
    // 1. Se esta tela tem filtro na barra de enderecos, guarda o endereco.
    if (TELAS_COM_FILTRO.includes(location.pathname)
        && location.search.includes("f=1")) {
      sessionStorage.setItem(chave(location.pathname),
                             location.pathname + location.search);
    }
    // 2. E aponta os links do menu para o endereco guardado de cada tela.
    document.querySelectorAll("a.topo-aba").forEach(link => {
      const caminho = new URL(link.href, location.origin).pathname;
      if (!TELAS_COM_FILTRO.includes(caminho)) return;
      const guardado = sessionStorage.getItem(chave(caminho));
      if (guardado) link.href = guardado;
    });
  } catch (e) { /* aba anonima: segue com os links normais */ }
})();


// ---------------------------------------------------------------------------
// O LOTE GUARDADO NAO PODE FICAR ATRASADO
//
// O Lote agora fica guardado no navegador por cinco minutos, como as telas de
// leitura — e por isso ir e voltar entre ele e as Solicitacoes e imediato.
//
// So que o Lote e a tela onde se ALTERA coisa. Se a copia guardada aparecesse
// DEPOIS de uma salvada, ela mostraria o lote sem o que a pessoa acabou de
// fazer — e ela poderia salvar por cima do proprio trabalho. Seria pior do
// que a lentidao que estamos consertando.
//
// A regra do HTTP diz que um POST apaga a copia guardada do endereco, e todo
// salvamento do Lote e um POST para o proprio endereco. Mas depender de o
// navegador cumprir isso, quando o preco de nao cumprir e perder trabalho, e
// aposta que nao vale.
//
// Entao: a tela carrega a HORA em que o lote foi salvo. Guardamos aqui a
// ultima hora vista. Se a tela em frente veio DO CACHE e traz hora diferente
// da ultima vista, ela esta atrasada e se recarrega sozinha, uma vez.
//
// "Veio do cache" e conferido pelo tamanho transferido: zero bytes na rede
// significa que o navegador serviu a copia guardada. Sem essa condicao, a
// tela que volta de uma salvada — que e nova e traz hora nova — se
// recarregaria a toa a cada salvamento.
// ---------------------------------------------------------------------------
(function () {
  const cartao = document.getElementById("cartao-lote");
  if (!cartao) return;

  const CHAVE = "analisesps:lote:salvo-em";
  const JA_RECARREGOU = "analisesps:lote:recarregou";
  const agora = cartao.dataset.loteEm || "";

  try {
    const nav = performance.getEntriesByType("navigation")[0];
    const doCache = !!nav && nav.transferSize === 0 && nav.decodedBodySize > 0;
    const ultima = sessionStorage.getItem(CHAVE);

    if (doCache && ultima && agora !== ultima
        && sessionStorage.getItem(JA_RECARREGOU) !== ultima) {
      // Marca ANTES de recarregar: se a recarga trouxer a mesma hora velha
      // (servidor fora do ar, por exemplo), nao entra em ciclo.
      sessionStorage.setItem(JA_RECARREGOU, ultima);
      location.reload();
      return;
    }
    sessionStorage.setItem(CHAVE, agora);
    sessionStorage.removeItem(JA_RECARREGOU);
  } catch (e) { /* aba anonima: segue com a tela como veio */ }
})();


// ---------------------------------------------------------------------------
// O BOTAO DE ATUALIZAR, ao lado da hora da base
//
// Era preciso ir a Configuracoes so para aperta-lo. Quem olha a hora da base e
// acha que esta velha quer atualizar ALI, nao noutra tela.
// ---------------------------------------------------------------------------
(function () {
  const botao = document.getElementById("btn-atualizar-base");
  if (!botao) return;
  botao.addEventListener("click", async () => {
    botao.disabled = true;
    const rotulo = botao.textContent;
    botao.textContent = "Atualizando…";
    try {
      const r = await fetch(botao.dataset.url, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({modo: "sincronizar"})
      });
      const d = await r.json();
      if (!d.ok) {
        alert(d.erro || "Não consegui iniciar a atualização.");
        return;
      }
      // A atualizacao roda num processo separado: nao adianta esperar aqui.
      // A busca de 90 em 90 segundos avisa quando a base mudar.
      botao.textContent = "Atualizando…";
      alert("Atualização iniciada. Ela roda no servidor — pode continuar "
            + "trabalhando. Quando a base mudar, aparece o aviso para "
            + "recarregar.");
    } catch (e) {
      alert("Falhou a comunicação com o servidor: " + e);
    } finally {
      botao.disabled = false;
      botao.textContent = rotulo;
    }
  });
})();


// A ROLAGEM. Guardada enquanto se rola, e nao so ao sair: sair da tela pode
// ser fechar a aba, e ai nao ha momento de despedida.
(function () {
  let relogio = null;
  window.addEventListener("scroll", () => {
    clearTimeout(relogio);
    relogio = setTimeout(() => LEMBRAR.gravar("rolagem", window.scrollY), 150);
  }, {passive: true});

  window.addEventListener("DOMContentLoaded", () => {
    const onde = LEMBRAR.ler("rolagem");
    // Só repõe se houver para onde rolar — numa tela que encolheu, rolar para
    // um ponto que não existe mais deixa a pessoa olhando o vazio.
    if (onde && document.body.scrollHeight > onde + window.innerHeight / 2) {
      window.scrollTo(0, onde);
    }
  });
})();


(function () {
  const barra = document.getElementById("barra-acoes");
  if (!barra) return;

  const marcas = () => Array.from(document.querySelectorAll("input.marca"));

  // O que identifica a linha para a memoria da marcacao. No Lote e a chave da
  // linha (grupo + posicao + SP), porque la a mesma SP pode aparecer duas
  // vezes; nas Solicitacoes nao ha chave e vale o numero, que ja e unico.
  const chaveDaLinha = c => c.dataset.chave || c.value;
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

    // O TOTAL POR CONTA do que está marcado. É por conta que o dinheiro sai,
    // então é este número que diz se a remessa cabe — o total geral só diz se
    // ela é grande. Ordenado do maior para o menor: com seis contas, a que
    // importa é a que concentra.
    const contas = document.getElementById("ba-contas");
    if (contas) {
      const soma = new Map();
      sel.forEach(c => {
        const nome = (c.dataset.conta || "").trim() || "(sem conta)";
        soma.set(nome, (soma.get(nome) || 0)
                 + (parseFloat(c.dataset.valor || "0") || 0));
      });
      const partes = Array.from(soma.entries()).sort((a, b) => b[1] - a[1]);
      contas.textContent = partes
          .map(([nome, v]) => nome + ": " + moeda(v)).join("  ·  ");
      // Uma conta só não acrescenta nada ao total que já está acima.
      contas.hidden = partes.length < 2;
    }

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

    // E guarda o que está marcado, para a volta a esta tela trazer tudo de
    // novo. Vazio é apagado em vez de guardado: uma lista vazia guardada
    // sobrescreveria a marcação de uma volta anterior.
    //
    // GUARDA A CHAVE DA LINHA, NAO O NUMERO DA SP. No Lote a mesma SP pode
    // estar em dois grupos, e guardar o numero fazia a volta marcar as duas -
    // o dono relatou em 11/09/2026: "esta bagunçando".
    if (sel.length) LEMBRAR.gravar("marcadas", sel.map(chaveDaLinha));
    else LEMBRAR.esquecer("marcadas");
  }

  function reporMarcacao() {
    const guardadas = LEMBRAR.ler("marcadas");
    if (!guardadas || !guardadas.length) return;
    const querem = new Set(guardadas);
    let repostas = 0;
    marcas().forEach(c => {
      if (querem.has(chaveDaLinha(c))) { c.checked = true; repostas += 1; }
    });
    // A barra do alto mostra quantas e quanto somam — então a marcação
    // reposta nunca é invisível, e nenhum botão age sobre ela sem confirmar.
    if (repostas) atualizar();
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

  // AGIR SOBRE A SELECAO APAGA A MEMORIA DELA.
  //
  // A memoria existe para quem SAI da tela e VOLTA: o filtro, a rolagem e as
  // caixinhas voltam como estavam. Mas depois de uma acao a tela recarrega, e
  // repor a marcacao fazia as SPs voltarem marcadas DEPOIS de ja terem sido
  // tratadas. O dono reportou em 11/09/2026: "sao reaplicadas selecoes que ja
  // desmarquei; nao pode retroagir".
  //
  // E nao e so incomodo: uma marcacao que reaparece sozinha convida a agir
  // duas vezes sobre a mesma SP - agendar de novo, mandar ao lote de novo.
  //
  // A tela de QR NAO chama isto de proposito: ela nao altera nada, so abre
  // outra tela, e quem volta de la quer a selecao inteira de volta.
  function selecaoConsumida() {
    try { LEMBRAR.esquecer("marcadas"); } catch (e) { /* aba anonima */ }
  }

  // --- Alterar coluna (status de pagamento e agendamento) ------------------
  barra.querySelectorAll("button[data-coluna]").forEach(botao => {
    botao.addEventListener("click", async () => {
      const ids = idsMarcados();
      if (!ids) return;
      selecaoConsumida();
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

  // --- BeeVale das marcadas ------------------------------------------------
  //
  // So habilita quando TODAS as marcadas sao BeeVale, como no Streamlit. Nao e
  // preciosismo: gerar a planilha de recarga de uma SP que se paga por boleto
  // poe dinheiro no cartao de quem nao devia receber, e o card fica marcado
  // como resolvido.
  const btnBeeVale = document.getElementById("ba-beevale");
  if (btnBeeVale) btnBeeVale.addEventListener("click", () => {
    const sel = marcadas();
    if (!sel.length) return;
    const forasteiras = sel.filter(
      c => !(c.dataset.forma || "").toLowerCase().includes("beevale"));
    if (forasteiras.length) {
      alert("O BeeVale so vale para SPs cuja forma de pagamento e BeeVale.\n\n"
            + forasteiras.length + " das marcadas nao sao ("
            + forasteiras.slice(0, 5).map(c => c.value).join(", ")
            + (forasteiras.length > 5 ? "…" : "") + ").");
      return;
    }
    const volta = barra.dataset.origem || "";
    location.href = btnBeeVale.dataset.url + "?"
        + sel.map(c => "id=" + encodeURIComponent(c.value)).join("&")
        + (volta ? "&origem=" + encodeURIComponent(volta) : "");
  });

  // --- Mandar as marcadas para o lote --------------------------------------
  // NAO SAI DA TELA. Antes o botao mandava um formulario e a pessoa era
  // levada para o Lote — perdendo o filtro, a rolagem e a marcacao de quem so
  // queria separar um grupo e continuar conferindo a lista. Pedido do dono em
  // 09/09/2026: "mantenha-se em Solicitacoes, apenas avise que foi executada
  // a acao".
  const btnLote = document.getElementById("ba-enviar-lote");
  if (btnLote) btnLote.addEventListener("click", async () => {
    const ids = idsMarcados();
    if (!ids) return;
    selecaoConsumida();
    btnLote.disabled = true;
    try {
      const r = await fetch(barra.dataset.urlEnviarLote, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ids: ids})
      });
      const d = await r.json();
      if (!d.ok) { alert("Não deu certo: " + (d.erro || "erro desconhecido")); return; }
      recado(d.quantas + " SP(s) entraram no grupo \"" + d.titulo + "\" do lote.",
             barra.dataset.urlLote, "Ver o lote");
    } catch (e) { alert("Falhou a comunicação com o servidor: " + e); }
    finally { btnLote.disabled = false; }
  });

  // O aviso de que a acao saiu. Discreto, some sozinho, e leva um link para
  // quem quiser conferir — mas sem arrastar ninguem para outra tela.
  function recado(texto, endereco, rotulo) {
    document.querySelectorAll(".recado-acao").forEach(v => v.remove());
    const caixa = document.createElement("div");
    caixa.className = "recado-acao";
    caixa.textContent = texto + " ";
    if (endereco) {
      const link = document.createElement("a");
      link.href = endereco;
      link.textContent = rotulo || "Ver";
      caixa.appendChild(link);
    }
    document.body.appendChild(caixa);
    setTimeout(() => caixa.classList.add("saindo"), 6000);
    setTimeout(() => caixa.remove(), 6600);
  }

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
    selecaoConsumida();
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
    selecaoConsumida();
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

  // A ORDEM importa: repor primeiro, e `reporMarcacao` chama `atualizar` só
  // quando repôs alguma coisa. Sem marcação guardada, a tela abre limpa.
  reporMarcacao();
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
