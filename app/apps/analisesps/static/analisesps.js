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


/* ---------------------------------------------------------------------------
   COMPROVANTES — arrastar e soltar, e a tela se atualizando sozinha.

   O dono pediu em 11/09/2026: "eu arrasto esses comprovantes pra dentro da
   tela e dispara a automacao". E logo depois: "se eu sair da tela e voltar, a
   informacao vai ser me dada ainda?".

   VAI. O resultado NAO mora aqui: mora no banco. Este arquivo so mostra. Se a
   pessoa fechar a aba no meio, o processo separado continua, e ao voltar ela
   ve tudo. Por isso aqui nao se guarda nada em memoria de pagina.
--------------------------------------------------------------------------- */
(function () {
  const area = document.getElementById("area-solta");
  const campo = document.getElementById("campo-comprovantes");
  const lista = document.getElementById("escolhidos");
  const botao = document.getElementById("btn-enviar");
  if (!area || !campo) return;

  function descrever() {
    const arquivos = Array.from(campo.files || []);
    botao.disabled = arquivos.length === 0;
    if (!arquivos.length) { lista.hidden = true; lista.textContent = ""; return; }
    const mb = t => (t / (1024 * 1024)).toFixed(1).replace(".", ",");
    lista.hidden = false;
    lista.textContent = arquivos.length + " arquivo(s): "
        + arquivos.map(a => `${a.name} (${mb(a.size)} MB)`).join(" · ");
  }

  // `dragover` PRECISA do preventDefault, senao o navegador abre o PDF numa
  // aba nova em vez de deixar soltar aqui — e a pessoa perde o que arrastou.
  ["dragenter", "dragover"].forEach(evento => {
    area.addEventListener(evento, e => {
      e.preventDefault();
      area.classList.add("por-cima");
    });
  });
  ["dragleave", "drop"].forEach(evento => {
    area.addEventListener(evento, e => {
      e.preventDefault();
      area.classList.remove("por-cima");
    });
  });

  area.addEventListener("drop", e => {
    const soltos = Array.from((e.dataTransfer && e.dataTransfer.files) || []);
    const pdfs = soltos.filter(a => /\.pdf$/i.test(a.name));
    if (!pdfs.length) {
      alert("Solte arquivos PDF. Comprovante em foto ou print ainda nao e "
            + "aceito por aqui.");
      return;
    }
    if (pdfs.length < soltos.length) {
      alert((soltos.length - pdfs.length) + " arquivo(s) que nao sao PDF "
            + "ficaram de fora.");
    }
    // DataTransfer e o unico jeito de por arquivos soltos dentro do <input>,
    // que e quem o formulario envia. Sem isto o arrastar nao manda nada.
    const saco = new DataTransfer();
    pdfs.forEach(a => saco.items.add(a));
    campo.files = saco.files;
    descrever();
  });

  campo.addEventListener("change", descrever);

  // O botao so pode ser apertado uma vez: dois envios do mesmo arquivo criariam
  // dois lotes. A baixa duplicada e barrada pelo robo, mas a tela ficaria com
  // duas linhas dizendo a mesma coisa, e isso confunde na hora de conferir.
  const form = document.getElementById("form-comprovantes");
  if (form) form.addEventListener("submit", () => {
    botao.disabled = true;
    botao.textContent = "Mandando…";
  });

  // Enquanto houver lote na fila ou processando, a tela se atualiza sozinha.
  // Quem diz se HA trabalho e o servidor, num atributo — nao o texto da
  // pagina. E perguntar so o andamento (consulta curta) em vez de recarregar
  // tudo: o banco tem um decimo de um nucleo.
  const estado = document.getElementById("comprovantes-estado");
  if (estado && estado.dataset.trabalhando) {
    let tentativas = 0;
    const relogio = setInterval(async () => {
      // Para de perguntar depois de ~10 minutos. Uma tela esquecida aberta a
      // noite inteira nao pode ficar batendo no banco para sempre.
      if (++tentativas > 120) { clearInterval(relogio); return; }
      try {
        const r = await fetch(estado.dataset.urlEstado, {
          headers: {"Accept": "application/json"}});
        const dados = await r.json();
        const parado = !dados.rodando && !(dados.lotes || []).some(
            l => l.situacao === "ESPERANDO" || l.situacao === "RODANDO");
        // Recarrega UMA vez quando tudo terminou, para mostrar as linhas.
        if (parado) { clearInterval(relogio); location.reload(); }
      } catch (e) { /* rede caiu: a proxima tentativa resolve */ }
    }, 5000);
  }
})();


/* ---------------------------------------------------------------------------
   DOCUMENTACAO FISCAL — as duas pilhas.

   O que o sistema propoe com confianca ja vem MARCADO pelo servidor; o que tem
   duvida vem desmarcado. Este arquivo NAO decide nada disso — ele so conta o
   que esta marcado e manda. A decisao de marcar ou nao e do servidor, onde a
   regra mora, e nao da tela.

   POR QUE ISSO IMPORTA: propor cria fadiga de aprovacao. Se a tela pudesse
   marcar tudo "para facilitar", em tres semanas ninguem conferiria mais — o
   mesmo olho cansado, so que mais rapido.
--------------------------------------------------------------------------- */
(function () {
  const config = document.getElementById("fiscal-config");
  const botao = document.getElementById("btn-confirmar-fiscal");
  if (!config || !botao) return;

  const marcas = () => Array.from(document.querySelectorAll(".fiscal-marca"));
  const marcadas = () => marcas().filter(c => c.checked);
  const contador = document.getElementById("fiscal-quantas");
  const todas = document.getElementById("fiscal-todas");

  // "Confirmar" so age no que TEM proposta; a IA age em qualquer marcada, e e
  // justamente nas SEM proposta que ela serve. Sao dois conjuntos diferentes
  // sobre as mesmas caixinhas.
  const comProposta = () => marcadas().filter(c => c.dataset.documentacao);
  const botaoIA = document.getElementById("btn-ia-fiscal");

  function atualizar() {
    const quantas = marcadas().length;
    const propostas = comProposta().length;
    botao.disabled = propostas === 0;
    if (botaoIA) botaoIA.disabled = quantas === 0;
    if (contador) {
      contador.textContent = quantas === 0 ? "Nenhuma marcada"
          : quantas + (quantas === 1 ? " marcada" : " marcadas")
            + (propostas < quantas
               ? ` (${propostas} com proposta)` : "");
    }
    if (todas) {
      const total = marcas().length;
      todas.checked = total > 0 && quantas === total;
      todas.indeterminate = quantas > 0 && quantas < total;
    }
  }

  marcas().forEach(c => c.addEventListener("change", atualizar));
  if (todas) todas.addEventListener("change", () => {
    marcas().forEach(c => { c.checked = todas.checked; });
    atualizar();
  });
  atualizar();

  // --- Mandar para a IA ler o anexo -----------------------------------------
  //
  // NUNCA automatico: quem escolhe e o dono, SP a SP. E a confirmacao diz o
  // que custa, porque cada leitura e cobrada — um clique distraido em duzentas
  // linhas seria uma conta que ninguem pediu.
  if (botaoIA) botaoIA.addEventListener("click", async () => {
    const ids = marcadas().map(c => c.dataset.sp);
    const semAnexo = marcadas().filter(c => !c.dataset.anexo).length;
    if (!ids.length) return;
    let recado = `Ler o anexo de ${ids.length} SP(s) com IA.\n\n`
        + `Cada leitura e cobrada. A IA PROPOE — quem confirma continua sendo `
        + `voce.`;
    if (semAnexo) {
      recado += `\n\nATENCAO: ${semAnexo} nao tem anexo e vao ser puladas.`;
    }
    if (!confirm(recado)) return;

    botaoIA.disabled = true;
    const antes = botaoIA.textContent;
    botaoIA.textContent = "Mandando…";
    try {
      const r = await fetch(config.dataset.urlIa, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ids}),
      });
      const dados = await r.json();
      if (!dados.ok) { alert(dados.erro || "Nao consegui enfileirar."); return; }
      alert(dados.aviso);
      location.reload();
    } catch (e) {
      alert("Nao consegui falar com o servidor: " + e);
    } finally {
      botaoIA.disabled = false;
      botaoIA.textContent = antes;
    }
  });

  botao.addEventListener("click", async () => {
    const itens = comProposta().map(c => ({
      sp: c.dataset.sp,
      documentacao: c.dataset.documentacao,
      chave: c.dataset.chave || "",
      confianca: parseInt(c.dataset.confianca || "0", 10) || 0,
      motivo: c.dataset.motivo || "",
    }));
    if (!itens.length) return;

    // A CONFIRMACAO DIZ O QUE VAI ACONTECER E O QUE NAO VAI. "Confirmar" numa
    // tela fiscal soa como "ja foi para a contabilidade"; aqui ainda nao foi
    // nem para o card.
    if (!confirm(`Confirmar a analise de ${itens.length} SP(s).\n\n`
                 + `Isto grava a decisao aqui. A gravacao nos cards do Pipefy `
                 + `e o passo seguinte.`)) return;

    botao.disabled = true;
    const texto = botao.textContent;
    botao.textContent = "Gravando…";
    try {
      const r = await fetch(config.dataset.urlConfirmar, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({itens}),
      });
      const dados = await r.json();
      if (!dados.ok) { alert(dados.erro || "Nao consegui gravar."); return; }
      if ((dados.recusadas || []).length) {
        alert(`${dados.gravadas} gravada(s). Estas ficaram de fora:\n`
              + dados.recusadas.join("\n"));
      }
      location.reload();
    } catch (e) {
      alert("Nao consegui falar com o servidor: " + e);
    } finally {
      botao.disabled = false;
      botao.textContent = texto;
    }
  });
})();


/* ---------------------------------------------------------------------------
   DOCUMENTACAO FISCAL — as acoes que moram DENTRO da tela de trabalho.

   Correcao do dono em 13/09/2026: *"ao buscar na Receita as notas emitidas
   contra a BWS, nao tem absolutamente nada a ver eu estar com um botao desse
   fora da tela de trabalho. (...) Ler, e pra estar dentro da tela. Gravar nos
   cards, e pra estar dentro da tela. Eu estou trabalhando la, estou tratando
   la, e vou operacionalizar por la."*

   Este bloco cuida de tres coisas, e vive SEPARADO do bloco das duas pilhas
   acima porque as duas visoes (por lancamento e por nota) usam partes
   diferentes: a visao por nota nao tem caixinhas nem botao de confirmar, e o
   bloco de cima desiste logo no comeco quando nao encontra o botao dele.

     1. disparar as tarefas longas e acompanhar o andamento;
     2. escrever a documentacao A MAO, quando o sistema nao propos nada;
     3. associar uma nota orfa a uma SP, do lado da nota.
--------------------------------------------------------------------------- */
(function () {
  const config = document.getElementById("fiscal-config");
  if (!config || !config.dataset.urlTarefa) return;

  // --- 1. As tarefas longas ------------------------------------------------
  //
  // O andamento vem do BANCO, nao da memoria de um processo: continua certo
  // mesmo se o servico reiniciar no meio, e mesmo se a tela for aberta de
  // outro aparelho. E o mesmo arranjo da tela de Configuracoes.
  const painelAndamento = document.getElementById("andamento-fiscal");
  let relogio = null;

  function acompanhar() {
    if (!painelAndamento || !config.dataset.urlAndamento) return;
    if (relogio) clearInterval(relogio);
    // SO RECARREGA DEPOIS DE TER VISTO A TAREFA VIVA. Entre o clique e a
    // tarefa aparecer no banco passa um instante; sem esta trava, a primeira
    // resposta ("nao ha nada rodando") recarregaria a tela na hora e daria a
    // impressao de que o botao nao fez nada.
    let viuRodando = false;
    relogio = setInterval(async () => {
      try {
        const r = await fetch(config.dataset.urlAndamento);
        const d = await r.json();
        if (d.rodando) {
          viuRodando = true;
          painelAndamento.innerHTML =
              '<div class="aviso"><b>Rodando agora:</b> '
              + (d.etapa || "") + (d.progresso ? " — " + d.progresso : "")
              + '<br><small>Pode continuar trabalhando: isto roda no servidor.'
              + '</small></div>';
        } else if (viuRodando) {
          // Terminou: a tela precisa ser relida, porque os numeros e a lista
          // mudaram. Quem disparou uma busca de notas esta esperando
          // justamente por isso.
          clearInterval(relogio);
          relogio = null;
          location.reload();
        }
      } catch (e) { /* rede oscilou; a proxima volta tenta de novo */ }
    }, 4000);
  }

  document.querySelectorAll(".fiscal-acoes-tarefa button[data-modo]")
      .forEach(b => b.addEventListener("click", async () => {
    const rotulo = b.textContent.trim();
    if (!confirm(`${rotulo}?\n\nIsto roda no servidor e pode demorar. `
                 + `Voce pode continuar trabalhando.`)) return;
    b.disabled = true;
    const antes = b.textContent;
    b.textContent = "Disparando…";
    try {
      const r = await fetch(config.dataset.urlTarefa, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({modo: b.dataset.modo}),
      });
      const d = await r.json();
      if (!d.ok) { alert(d.erro || "Nao consegui disparar."); return; }
      if (painelAndamento) {
        painelAndamento.innerHTML =
            '<div class="aviso"><b>Disparado:</b> ' + rotulo
            + '<br><small>Pode continuar trabalhando: isto roda no servidor.'
            + '</small></div>';
      }
      acompanhar();
    } catch (e) {
      alert("Nao consegui falar com o servidor: " + e);
    } finally {
      b.disabled = false;
      b.textContent = antes;
    }
  }));

  // Ja havia carga rodando quando a tela abriu: acompanha sem esperar clique.
  if (painelAndamento && painelAndamento.querySelector(".aviso")) acompanhar();

  // --- 2. Escrever a documentacao A MAO ------------------------------------
  //
  // O buraco que ele achou usando a tela: *"tudo aquilo que voce sugeriu (...)
  // mas o que voce nao sugeriu, como e que eu adiciono a informacao? Porque a
  // planilha ela me permite adicionar, e a tela nao permite."*
  //
  // UMA janela so, preenchida pela linha em que se clicou. Duzentas janelas
  // escondidas na pagina custariam memoria de navegador a toa.
  const dlg = document.getElementById("dlg-mao");
  const campoDoc = document.getElementById("mao-documentacao");
  const campoChave = document.getElementById("mao-chave");
  const alvoQuem = document.getElementById("mao-quem");
  const alvoNota = document.getElementById("mao-nota");
  const alvoErro = document.getElementById("mao-erro");
  let spAtual = "";

  function recado(texto, classe) {
    if (!alvoErro) return;
    alvoErro.innerHTML = texto
        ? '<div class="aviso ' + (classe || "atencao") + '">' + texto + '</div>'
        : "";
  }

  if (dlg) {
    document.querySelectorAll(".fiscal-mao").forEach(b =>
      b.addEventListener("click", () => {
        spAtual = b.dataset.sp;
        if (alvoQuem) {
          alvoQuem.textContent = `SP ${b.dataset.sp} · ${b.dataset.credor}`
              + ` · ${b.dataset.valor}`;
        }
        if (campoDoc) campoDoc.value = b.dataset.documentacao || "";
        if (campoChave) campoChave.value = b.dataset.chave || "";
        if (alvoNota) alvoNota.textContent = "";
        recado("");
        dlg.showModal();
        conferirChave();
      }));

    // CONFERIR A CHAVE ENQUANTO SE DIGITA. Uma chave errada nao da erro: ela
    // grava no card uma nota que nao e a da despesa, e ninguem descobre — e e
    // exatamente esse o defeito que esta tela existe para achar. Dizer de quem
    // e a nota ANTES de gravar e o que transforma digitacao em conferencia.
    let esperaChave = null;
    async function conferirChave() {
      if (!campoChave || !alvoNota || !config.dataset.urlNota) return;
      const digitos = (campoChave.value || "").replace(/\D/g, "");
      if (digitos.length !== 44) {
        alvoNota.textContent = digitos.length
            ? `${digitos.length} de 44 numeros` : "";
        return;
      }
      try {
        const r = await fetch(config.dataset.urlNota + "?chave="
                              + encodeURIComponent(digitos));
        const d = await r.json();
        if (d.nota) {
          alvoNota.textContent = `Nota ${d.nota.numero} — ${d.nota.emitente}`
              + (d.nota.status ? ` (${d.nota.status})` : "");
        } else {
          // NAO E ERRO: a nota pode ainda nao ter sido importada. Dizer que
          // ela "nao existe" faria a pessoa desistir de uma chave correta.
          alvoNota.textContent = "Esta chave ainda nao esta na lista de notas "
              + "daqui — pode ser nota que ainda nao foi importada.";
        }
      } catch (e) { alvoNota.textContent = ""; }
    }
    if (campoChave) campoChave.addEventListener("input", () => {
      if (esperaChave) clearTimeout(esperaChave);
      esperaChave = setTimeout(conferirChave, 400);
    });

    const btnCancelar = document.getElementById("btn-mao-cancelar");
    if (btnCancelar) btnCancelar.addEventListener("click", e => {
      e.preventDefault();
      dlg.close();
    });

    const btnGravar = document.getElementById("btn-mao-gravar");
    if (btnGravar) btnGravar.addEventListener("click", async e => {
      e.preventDefault();
      btnGravar.disabled = true;
      const antes = btnGravar.textContent;
      btnGravar.textContent = "Gravando…";
      try {
        const r = await fetch(config.dataset.urlMao, {
          method: "POST", headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            sp: spAtual,
            documentacao: campoDoc ? campoDoc.value : "",
            chave: campoChave ? campoChave.value : "",
          }),
        });
        const d = await r.json();
        if (!d.ok) { recado(d.erro || "Nao consegui gravar."); return; }
        dlg.close();
        location.reload();
      } catch (e2) {
        recado("Nao consegui falar com o servidor: " + e2);
      } finally {
        btnGravar.disabled = false;
        btnGravar.textContent = antes;
      }
    });
  }

  // --- 3. Associar uma nota orfa a uma SP ----------------------------------
  //
  // Do lado da NOTA. Passa pelo mesmo caminho da decisao a mao, entao a nota
  // associada some desta lista sozinha na proxima leitura — a lista de orfas e
  // "nota sem chave gravada em SP nenhuma".
  document.querySelectorAll(".fiscal-associar").forEach(b =>
    b.addEventListener("click", async () => {
      if (!confirm(`Associar esta nota a SP ${b.dataset.sp}`
                   + ` (${b.dataset.credor})?\n\n`
                   + `A categoria sai de dentro da propria chave. A gravacao `
                   + `no card do Pipefy e o passo seguinte.`)) return;
      b.disabled = true;
      const antes = b.textContent;
      b.textContent = "gravando…";
      try {
        const r = await fetch(config.dataset.urlMao, {
          method: "POST", headers: {"Content-Type": "application/json"},
          body: JSON.stringify({sp: b.dataset.sp, chave: b.dataset.chave}),
        });
        const d = await r.json();
        if (!d.ok) { alert(d.erro || "Nao consegui associar."); return; }
        location.reload();
      } catch (e) {
        alert("Nao consegui falar com o servidor: " + e);
      } finally {
        b.disabled = false;
        b.textContent = antes;
      }
    }));
})();


/* ---------------------------------------------------------------------------
   A NAVEGACAO QUE CABE — 13/09/2026

   Reclamacao do dono: *"numa tela grande e tranquilo de navegar, porque todos
   aparecem, mas numa tela pequena ele fica escondido, as ultimas."*

   NAO HA LARGURA DE CORTE CHUTADA. A pagina MEDE: se a faixa de abas nao couber
   inteira, ela sai e entra o botao de menu, que lista todas as telas. Escolher
   um numero magico (900px? 1100px?) erraria sozinho, porque o canto direito do
   topo muda de tamanho conforme a tela — a hora da base e o botao "Atualizar"
   so aparecem em algumas. Medindo, acerta sempre.

   SEM ESTE SCRIPT nada quebra: a faixa fica como era, rolando de lado.
--------------------------------------------------------------------------- */
(function () {
  const topo = document.querySelector(".topo");
  const abas = document.getElementById("topo-abas");
  const botao = document.getElementById("btn-menu");
  const painel = document.getElementById("menu-painel");
  if (!topo || !abas || !botao || !painel) return;

  const ondeEstou = document.getElementById("btn-menu-onde");
  const ativa = abas.querySelector(".topo-aba.ativa");

  // O botao diz ONDE se esta, nao so "Menu". Numa tela pequena a faixa some, e
  // com ela some a unica marca de qual tela esta aberta.
  if (ondeEstou && ativa) ondeEstou.textContent = ativa.textContent.trim();

  function cabe() {
    // Medido com a faixa VISIVEL. Se ela estiver escondida (estado compacto),
    // e preciso mostra-la por um instante para saber se ja caberia de novo —
    // senao a tela nunca voltaria ao normal ao ser alargada.
    const compacto = topo.classList.contains("compacto");
    if (compacto) topo.classList.remove("compacto");
    const serve = abas.scrollWidth <= abas.clientWidth + 1;
    if (compacto && !serve) topo.classList.add("compacto");
    return serve;
  }

  function ajustar() {
    if (cabe()) {
      topo.classList.remove("compacto");
      fechar();
    } else {
      topo.classList.add("compacto");
      botao.hidden = false;
    }
  }

  function abrir() {
    painel.hidden = false;
    botao.setAttribute("aria-expanded", "true");
  }

  function fechar() {
    painel.hidden = true;
    botao.setAttribute("aria-expanded", "false");
  }

  botao.addEventListener("click", e => {
    e.stopPropagation();
    if (painel.hidden) abrir(); else fechar();
  });

  // Clicar fora fecha, e a tecla Esc tambem. Menu aberto que so fecha no
  // proprio botao e menu que fica aberto por engano.
  document.addEventListener("click", e => {
    if (!painel.hidden && !painel.contains(e.target)) fechar();
  });
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") fechar();
  });

  // Redimensionar dispara muito; esperar um instante evita medir dezenas de
  // vezes durante o arrasto da janela.
  let espera = null;
  window.addEventListener("resize", () => {
    if (espera) clearTimeout(espera);
    espera = setTimeout(ajustar, 120);
  });

  ajustar();
})();


/* ---------------------------------------------------------------------------
   RECONFERIR — "como e que eu sei que isso esta sendo analisado?"

   Pergunta do dono em 13/09/2026, e ela tinha razao de ser: a conferencia SEMPRE
   rodou a cada abertura da tela, inclusive sobre o que ja foi decidido e ja foi
   gravado no card — e a tela nunca disse isso em lugar nenhum. O que nao e dito
   nao existe para quem usa.

   O que faltava de verdade era mandar reconferir REGISTROS ESCOLHIDOS, sem
   esperar a proxima abertura. E o que este bloco faz.

   NAO GRAVA NADA e nao custa IA: e so perguntar de novo. Por isso nao pede
   confirmacao — o custo de um clique errado aqui e zero.
--------------------------------------------------------------------------- */
(function () {
  const config = document.getElementById("fiscal-config");
  if (!config || !config.dataset.urlReconferir) return;
  const alvo = document.getElementById("fiscal-reconferencia");

  function linha(i) {
    const mudou = i.mudou
        ? '<b>mudou:</b> estava "' + i.antes + '", proponho "' + i.proposta + '"'
        : '<b>sem mudanca:</b> continua "' + i.antes + '"';
    const nota = i.nota && i.nota.numero
        ? ' · nota ' + i.nota.numero + ' de ' + (i.nota.emitente || "")
          + (i.nota.status ? " (" + i.nota.status + ")" : "")
        : "";
    return '<div class="reconf-item"><b>SP ' + i.sp + '</b> — ' + i.rotulo
         + '<br>' + mudou + nota
         + '<br><small>' + (i.motivo || "") + '</small></div>';
  }

  async function reconferir(ids, botao) {
    if (!ids.length || !alvo) return;
    const antes = botao ? botao.textContent : "";
    if (botao) { botao.disabled = true; botao.textContent = "Conferindo…"; }
    alvo.innerHTML = '<div class="aviso">Reconferindo ' + ids.length
                   + ' lancamento(s)…</div>';
    try {
      const r = await fetch(config.dataset.urlReconferir, {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ids}),
      });
      const d = await r.json();
      if (!d.ok) {
        alvo.innerHTML = '<div class="aviso atencao">' + (d.erro || "Falhou.")
                       + '</div>';
        return;
      }
      // A CONCLUSAO VEM PRIMEIRO. Trinta paragrafos sem uma frase no alto
      // seriam trinta paragrafos que ninguem le.
      const cabeca = '<div class="aviso ' + (d.mudaram ? "atencao" : "ok") + '">'
          + '<b>Reconferidos ' + d.itens.length + '</b> agora, contra as notas '
          + 'que existem hoje. ' + d.mudaram + ' com proposta diferente do que '
          + 'esta gravado; ' + d.apontados + ' com alguma coisa a apontar.'
          + (d.faltaram && d.faltaram.length
             ? '<br><small>Nao achei na base: ' + d.faltaram.join(", ")
               + '</small>' : "")
          + '<br><small>Nada foi gravado — isto e so a conferencia.</small>'
          + '</div>';
      alvo.innerHTML = cabeca + d.itens.map(linha).join("");
      alvo.scrollIntoView({behavior: "smooth", block: "nearest"});
    } catch (e) {
      alvo.innerHTML = '<div class="aviso atencao">Nao consegui falar com o '
                     + 'servidor: ' + e + '</div>';
    } finally {
      if (botao) { botao.disabled = false; botao.textContent = antes; }
    }
  }

  const botaoLote = document.getElementById("btn-reconferir");
  if (botaoLote) {
    const marcadas = () => Array.from(
        document.querySelectorAll(".fiscal-marca:checked"));
    // O botao acompanha a selecao, como os outros dois ao lado dele.
    const seguir = () => { botaoLote.disabled = marcadas().length === 0; };
    document.querySelectorAll(".fiscal-marca").forEach(
        c => c.addEventListener("change", seguir));
    const todas = document.getElementById("fiscal-todas");
    if (todas) todas.addEventListener("change", () => setTimeout(seguir, 0));
    seguir();
    botaoLote.addEventListener("click", () =>
        reconferir(marcadas().map(c => c.dataset.sp), botaoLote));
  }

  document.querySelectorAll(".fiscal-reconferir").forEach(b =>
    b.addEventListener("click", () => reconferir([b.dataset.sp], b)));
})();


/* ---------------------------------------------------------------------------
   VER OS DADOS — a prova por tras da proposta.

   Cobranca do dono em 13/09/2026: *"voce sugere e eu quero ver de forma
   completa os dados do que voce esta sugerindo. Os dados do relatorio FSist.
   Como faco? Ou quero ver os dados do registro, nao da pra ver pra validar.
   Isso pra eu ter que confiar somente no que voce observou."*

   A tela mostrava a CONCLUSAO e escondia o que a sustenta. Numa tela cujo
   trabalho e achar erro, quem confere sem poder ver vira carimbo — e carimbo
   nao acha nada.

   O QUE APARECE: a SP inteira, a nota inteira, e a conta dos pontos regra a
   regra — INCLUSIVE as que nao pontuaram, que sao as que explicam por que a
   confianca nao foi maior. E todas as candidatas, nao so a vencedora: ver a
   segunda colocada e o que permite discordar da escolha.
--------------------------------------------------------------------------- */
(function () {
  const config = document.getElementById("fiscal-config");
  const dlg = document.getElementById("dlg-provas");
  if (!config || !dlg || !config.dataset.urlComparar) return;
  const corpo = document.getElementById("provas-corpo");
  const titulo = document.getElementById("provas-sp");

  const esc = s => String(s === null || s === undefined ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

  const dinheiro = v => (v === null || v === undefined || v === "")
      ? "—"
      : Number(v).toLocaleString("pt-BR",
          {minimumFractionDigits: 2, maximumFractionDigits: 2});

  function ficha(titulo, campos) {
    return '<div class="prova-ficha"><h4>' + esc(titulo) + '</h4><dl>'
      + campos.map(c => '<dt>' + esc(c.rotulo) + '</dt><dd>'
                        + (esc(c.valor) || "—") + '</dd>').join("")
      + '</dl></div>';
  }

  // A CONTA ABERTA. Cada regra com o que tem de um lado e do outro, quanto
  // vale, e se entrou ou nao. E a parte que responde "por que 35%?".
  function conta(regras) {
    const linha = r => {
      const marca = r.bateu ? "✔" : (r.quase ? "≈" : "✕");
      const classe = r.bateu ? "bateu" : (r.quase ? "quase" : "falhou");
      let lado;
      if (r.chave === "valor") {
        lado = "SP " + dinheiro(r.no_lancamento) + " · nota "
             + dinheiro(r.na_nota)
             + (r.diferenca ? " · diferenca " + dinheiro(r.diferenca) : "");
      } else {
        lado = "SP: " + (esc(r.no_lancamento) || "—")
             + " · nota: " + (esc(r.na_nota) || "—");
      }
      const ganhou = r.bateu ? "+" + r.pontos
                   : (r.quase ? "parcial" : "0 de " + r.pontos);
      return '<tr class="' + classe + '"><td>' + marca + '</td><td>'
           + esc(r.rotulo) + '<br><small>' + lado + '</small></td>'
           + '<td class="num">' + ganhou + '</td></tr>';
    };
    return '<table class="prova-conta"><tbody>'
         + regras.map(linha).join("") + '</tbody></table>';
  }

  function candidata(c, i, corte) {
    const cabeca = '<div class="prova-cab"><b>'
        + (i === 0 ? "Melhor candidata" : "Candidata " + (i + 1)) + '</b>'
        + ' — confianca <b>' + c.pontos + '%</b>'
        + (c.pontos >= corte ? ' <span class="etiqueta boa">acima do corte</span>'
                             : ' <span class="etiqueta">abaixo do corte de '
                               + corte + '%</span>')
        + (c.categoria_pela_chave
           ? ' · a chave diz que e <b>' + esc(c.categoria_pela_chave) + '</b>'
           : "")
        + '</div>';
    return '<div class="prova-candidata">' + cabeca
         + '<div class="prova-lados">' + ficha("A nota (como está guardada)", c.campos)
         + '<div class="prova-ficha"><h4>Como os pontos foram contados</h4>'
         + conta(c.regras) + '</div></div>'
         // USAR ESTA NOTA: fecha o par com a candidata que a PESSOA escolheu,
         // e nao so com a que o sistema pos em primeiro. E o que transforma
         // "discordo" em trabalho feito, em vez de reclamacao.
         + (config.dataset.urlMao
            ? '<button class="btn secundario prova-usar" data-chave="'
              + esc(c.chave) + '">Usar esta nota nesta SP</button>' : "")
         + '</div>';
  }

  function desenhar(d) {
    if (!d.ok) {
      corpo.innerHTML = '<div class="aviso atencao">'
                      + esc(d.erro || "Não consegui carregar.") + '</div>';
      return;
    }
    const v = d.veredito || {};
    const j = d.diario || {};
    let html = '<div class="aviso ' + (v.propoe ? "info" : "") + '">'
        + '<b>' + esc(v.rotulo || "") + '</b> — ' + esc(v.motivo || "")
        + (v.proposta ? '<br>Proposta: <b>' + esc(v.proposta) + '</b> · '
                      + 'confianca ' + v.confianca + '%' : "")
        + '<br><small>Olhei <b>' + d.olhadas + '</b> nota(s) deste credor. '
        + 'O sistema so propoe a partir de ' + d.corte + '% de confianca.</small>'
        + '</div>';

    if (j.documentacao || j.chave) {
      html += '<div class="prova-diario">O que ja esta gravado: <b>'
           + (esc(j.documentacao) || "sem categoria") + '</b>'
           + (j.chave ? ' · chave ' + esc(j.chave) : "")
           + (j.origem ? ' · origem ' + esc(j.origem) : "")
           + (j.por ? ' · por ' + esc(j.por) : "") + '</div>';
    }

    html += ficha("O lançamento (a SP inteira)", d.lancamento || []);

    if (!d.candidatas || !d.candidatas.length) {
      html += '<div class="aviso atencao">Nenhuma nota deste credor foi '
            + 'encontrada na base. Isso nao quer dizer que ela nao exista — '
            + 'pode ser nota que ainda nao foi importada, ou emitida por outro '
            + 'CNPJ do mesmo grupo.</div>';
    } else {
      html += d.candidatas.map((c, i) => candidata(c, i, d.corte)).join("");
    }
    corpo.innerHTML = html;

    corpo.querySelectorAll(".prova-usar").forEach(b =>
      b.addEventListener("click", async () => {
        if (!confirm("Gravar esta nota nesta SP?\n\nA categoria sai de dentro "
                     + "da propria chave. A gravacao no card do Pipefy e o "
                     + "passo seguinte.")) return;
        b.disabled = true;
        try {
          const r = await fetch(config.dataset.urlMao, {
            method: "POST", headers: {"Content-Type": "application/json"},
            body: JSON.stringify({sp: d.sp, chave: b.dataset.chave}),
          });
          const resp = await r.json();
          if (!resp.ok) { alert(resp.erro || "Nao consegui gravar."); return; }
          location.reload();
        } catch (e) {
          alert("Nao consegui falar com o servidor: " + e);
        } finally { b.disabled = false; }
      }));
  }

  document.querySelectorAll(".fiscal-provas").forEach(b =>
    b.addEventListener("click", async () => {
      if (titulo) titulo.textContent = "SP " + b.dataset.sp;
      corpo.innerHTML = '<p class="cartao-dica">Carregando…</p>';
      dlg.showModal();
      try {
        const r = await fetch(config.dataset.urlComparar + "?sp="
                              + encodeURIComponent(b.dataset.sp));
        desenhar(await r.json());
      } catch (e) {
        corpo.innerHTML = '<div class="aviso atencao">Nao consegui falar com o '
                        + 'servidor: ' + e + '</div>';
      }
    }));
})();


/* ---------------------------------------------------------------------------
   TODO BOTAO QUE FAZ ALGUMA COISA TEM DE DIZER QUE ESTA FAZENDO.

   Reclamacao do dono em 13/09/2026, e ele diz que e geral: *"eu estou vendo que
   e muito comum acontecer isso: os botoes que deveriam, apos o clique,
   determinar alguma acao, ou mostrar a acao que esta sendo executada, ele nao
   mostra. Voce fica cego, sem saber se esta acontecendo alguma coisa ou nao."*

   Ele tem razao, e o caso que mais dói e o do formulario que recarrega a
   pagina: entre o clique e a tela voltar podem passar VARIOS SEGUNDOS — a
   equalizacao de credor reescreve centenas de SPs, uma de cada vez, pelo mesmo
   caminho que grava banco, fila, log e planilha. Nesse intervalo a tela fica
   exatamente igual, e quem clicou conclui que o botao nao funcionou. Aí clica
   de novo.

   ESTE BLOCO NAO SABE O QUE CADA BOTAO FAZ, e nao precisa: ele so trata o
   envio de formulario, que e o momento em que a pagina vai embora e nao volta
   na hora. Um por um, cada tela teria de lembrar — e e por isso que este
   defeito aparecia em tantos lugares.

   O QUE ELE NAO FAZ: mexer em botao de JavaScript (os que chamam o servidor
   por tras e ja tratam o proprio estado), nem em formulario de filtro, que se
   reenvia sozinho a cada caixa marcada e ficaria piscando "Aguarde" o tempo
   todo.
--------------------------------------------------------------------------- */
(function () {
  const PALAVRA = "Aguarde…";

  function ocupar(botao) {
    if (!botao || botao.dataset.ocupado) return;
    botao.dataset.ocupado = "1";
    // A LARGURA E TRAVADA ANTES de trocar o texto: sem isso o botao encolhe ou
    // cresce no meio do clique, e a tela "pula" na cara de quem apertou.
    const caixa = botao.getBoundingClientRect();
    if (caixa.width) botao.style.minWidth = Math.ceil(caixa.width) + "px";
    botao.dataset.textoAntes = botao.textContent;
    botao.textContent = PALAVRA;
    botao.classList.add("ocupado");
    // `disabled` num botao de submit CANCELA o envio em alguns navegadores se
    // aplicado cedo demais; por isso o desligamento espera o proximo quadro.
    setTimeout(() => { botao.disabled = true; }, 0);
  }

  document.addEventListener("submit", e => {
    const form = e.target;
    if (!(form instanceof HTMLFormElement)) return;
    // Formulario de filtro se reenvia sozinho o tempo todo: marcaria "Aguarde"
    // a cada caixa e viraria ruido.
    if (form.id === "form-filtros" || form.dataset.semAguarde) return;
    // O botao que de fato enviou, quando da para saber; senao, o primeiro.
    const botao = (e.submitter && e.submitter.tagName === "BUTTON")
        ? e.submitter
        : form.querySelector("button[type=submit], button:not([type])");
    ocupar(botao);

    // REDE CAIU OU O SERVIDOR DEMOROU DEMAIS: o botao volta ao normal depois
    // de um minuto. Deixar "Aguarde" para sempre numa tela que nao recarregou
    // seria trocar um engano por outro.
    setTimeout(() => {
      if (!botao || !botao.dataset.ocupado) return;
      botao.disabled = false;
      botao.textContent = botao.dataset.textoAntes || botao.textContent;
      botao.classList.remove("ocupado");
      delete botao.dataset.ocupado;
    }, 60000);
  }, true);
})();

/* ==========================================================================
   AS SPs POR TRÁS DE CADA NOME, na tela de credores.

   Pedido do dono em 13/09/2026: *"eu estou diante de um determinado CNPJ, aí
   aparecem várias opções (…) ele marca aqui uma, duas, três, quatro SPs que é
   de uma outra locadora que não tem nada a ver, ou seja, aqui foi claramente
   um erro. Só que a partir daqui eu não consigo ir a essas SPs que estão
   erradas. Só pra poder confirmar se eu posso realmente aplicar ou não, eu
   precisaria ver essas SPs e entender onde foi o erro."*

   A tela pedia decisão e escondia o dado da decisão. Abre POR CIMA, como todo
   o resto do módulo — sair da tela no meio de uma escolha perde a escolha.
   ========================================================================== */
(function () {
  var cfg = document.getElementById("credores-config");
  var caixa = document.getElementById("sps-do-nome");
  if (!cfg || !caixa) { return; }
  var titulo = document.getElementById("sps-do-nome-titulo");
  var corpo = document.getElementById("sps-do-nome-corpo");

  function escapar(t) {
    var d = document.createElement("div");
    d.textContent = t == null ? "" : String(t);
    return d.innerHTML;
  }

  function desenhar(dados, nome) {
    if (!dados.ok) {
      corpo.innerHTML = '<p class="aviso">Não consegui buscar as SPs: '
        + escapar(dados.erro || "erro desconhecido") + "</p>";
      return;
    }
    var sps = dados.sps || [];
    if (!sps.length) {
      corpo.innerHTML = '<p class="cartao-dica">Nenhuma SP escrita com este '
        + "nome. Se isso aparecer, a contagem da tela e a base discordam — "
        + "vale avisar.</p>";
      return;
    }
    var linhas = sps.map(function (s) {
      return "<tr><td class=\"id\">" + escapar(s.id) + "</td>"
        + "<td>" + escapar(s.credor) + "</td>"
        + "<td>" + escapar(s.valor) + "</td>"
        + "<td>" + escapar(s.vencimento) + "</td>"
        + "<td>" + escapar(s.status) + "</td>"
        + "<td class=\"cartao-dica\">" + escapar(s.descricao) + "</td>"
        + "<td>" + (s.card
          ? '<a href="' + escapar(s.card) + '" target="_blank" rel="noopener">card</a>'
          : "—") + "</td></tr>";
    }).join("");
    /* O TETO É DITO, e não escondido: uma lista cortada em silêncio faria a
       conferência concluir o contrário do que os dados dizem. */
    var aviso = sps.length >= (dados.teto || 50)
      ? '<p class="cartao-dica">Mostrando as ' + sps.length
        + " mais recentes. Há mais SPs com este nome.</p>"
      : "";
    corpo.innerHTML = '<p class="cartao-dica">' + sps.length
      + " SP(s) escritas como <b>" + escapar(nome) + "</b>.</p>"
      + '<div style="max-height:60vh; overflow:auto">'
      + '<table class="sps"><thead><tr><th>SP</th><th>Credor escrito</th>'
      + "<th>Valor</th><th>Vencimento</th><th>Pagamento</th><th>Descrição</th>"
      + "<th>Card</th></tr></thead><tbody>" + linhas + "</tbody></table></div>"
      + aviso;
  }

  document.addEventListener("click", function (ev) {
    var botao = ev.target.closest && ev.target.closest(".ver-sps-do-nome");
    if (!botao) { return; }
    /* O botão vive DENTRO do <label> da opção: sem isto, clicar em "ver as
       SPs" marcaria o rádio daquela opção — a tela decidiria por ele só por
       ele ter pedido para conferir. */
    ev.preventDefault();
    ev.stopPropagation();

    var nome = botao.dataset.nome || "";
    titulo.textContent = "SPs escritas como “" + nome + "”";
    corpo.innerHTML = '<p class="cartao-dica">Buscando…</p>';
    if (typeof caixa.showModal === "function") { caixa.showModal(); }

    var dados = new FormData();
    dados.append("documento", botao.dataset.documento || "");
    (botao.dataset.grafias || nome).split("\n").forEach(function (g) {
      if (g.trim()) { dados.append("grafia", g); }
    });

    fetch(cfg.dataset.urlSps, {
      method: "POST", body: dados, credentials: "same-origin"
    }).then(function (r) { return r.json(); })
      .then(function (d) { desenhar(d, nome); })
      .catch(function (e) {
        corpo.innerHTML = '<p class="aviso">Não consegui buscar as SPs: '
          + escapar(e && e.message) + "</p>";
      });
  });
})();
