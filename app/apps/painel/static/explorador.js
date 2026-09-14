// ---------------------------------------------------------------------------
// Editar a classificacao NA PROPRIA LISTA do Explorador
// ---------------------------------------------------------------------------
// Ate 13/09/2026 esta tela tinha DUAS listas: a de procurar, embaixo, e outra
// dentro de um bloco "Alterar no OMIE", com caixas de marcar. Quem usava tinha
// de achar na segunda o que viu na primeira. O dono foi direto: "basta uma
// lista e a gente vai trabalhar em cima dessa lista".
//
// Entao agora e uma so, e ela aceita os dois jeitos de trabalhar:
//
//   UM A UM      clicar na celula de Categoria ou de Obra e escolher a nova.
//   EM LOTE      marcar varias linhas, escolher uma vez la em cima, e vale
//                para todas as marcadas.
//
// Nada e enviado enquanto se edita: o que se monta aqui e um RASCUNHO, que so
// vira chamada ao OMIE quando alguem aperta Ensaiar ou Alterar de verdade.
//
// UM TITULO RATEADO APARECE EM VARIAS LINHAS (uma por obra). No OMIE ele e um
// cadastro so, entao editar qualquer uma das linhas edita o titulo inteiro, e
// as outras linhas dele acompanham na hora. Fingir que sao registros separados
// seria mentir sobre o que o botao faz.
(function () {
  const tabela = document.getElementById("lista-explorador");
  if (!tabela) return;

  const form = document.getElementById("form-alterar");
  const picker = document.getElementById("picker");
  const buscaPicker = picker.querySelector("[data-picker-busca]");
  const opcoesPicker = picker.querySelector("[data-picker-opcoes]");
  const tituloPicker = picker.querySelector("[data-picker-titulo]");

  const CATEGORIAS = JSON.parse(document.getElementById("dados-categorias").textContent);
  const OBRAS = JSON.parse(document.getElementById("dados-obras").textContent);

  // codigo do titulo -> {categoria:{codigo,rotulo}, obra:{codigo,rotulo}}
  const rascunhos = new Map();
  // o mesmo teto que `saneamento.py` aplica no servidor — nao e limite tecnico,
  // e para um engano de selecao nao virar um estrago de mil titulos
  const TETO_POR_LOTE = Number(tabela.dataset.teto || 200);
  let alvo = null;  // {campo: "categoria"|"obra", codigos: [...]}

  const simples = t => (t || "").normalize("NFD").replace(/[̀-ͯ]/g, "")
                                .toLowerCase().trim();
  const linhasDe = codigo =>
    tabela.querySelectorAll(`tbody tr[data-codigo="${CSS.escape(codigo)}"]`);

  // -------------------------------------------------------------------------
  // Desenhar o que esta rascunhado
  // -------------------------------------------------------------------------
  function pintar(codigo) {
    const rascunho = rascunhos.get(codigo) || {};
    linhasDe(codigo).forEach(tr => {
      tr.classList.toggle("com-rascunho", !!(rascunho.categoria || rascunho.obra));
      tr.querySelectorAll("[data-campo]").forEach(celula => {
        const novo = rascunho[celula.dataset.campo];
        const destino = celula.querySelector("[data-novo]");
        destino.textContent = novo ? novo.rotulo : "";
        destino.classList.toggle("oculto", !novo);
        // riscar so a celula que MUDOU. Riscar a linha inteira dizia que a obra
        // ia mudar quando so a categoria tinha sido trocada — e isso e mentira
        // sobre o que o botao vai fazer.
        celula.classList.toggle("trocada", !!novo);
      });
      const desfazer = tr.querySelector("[data-desfazer]");
      if (desfazer) desfazer.classList.toggle("oculto",
                                              !(rascunho.categoria || rascunho.obra));
    });
  }

  function contar() {
    const marcadas = tabela.querySelectorAll("tbody input[data-marca]:checked");
    const titulos = new Set([...marcadas].map(c => c.closest("tr").dataset.codigo));
    document.getElementById("conta-marcados").textContent =
      titulos.size === 0 ? "nenhum título marcado"
      : titulos.size === 1 ? "1 título marcado"
      : `${titulos.size} títulos marcados`;
    document.querySelectorAll("[data-precisa-marcado]").forEach(
      b => { b.disabled = titulos.size === 0; });

    const pendentes = rascunhos.size;
    const aviso = document.getElementById("conta-pendentes");
    aviso.textContent = pendentes === 0 ? "nenhuma alteração pendente"
      : pendentes === 1 ? "1 título com alteração pendente"
      : `${pendentes} títulos com alteração pendente`;
    // O servidor recusa lote acima do teto. Dizer isso DEPOIS de a pessoa ter
    // editado duzentas linhas seria cruel — o aviso aparece enquanto ela edita.
    if (pendentes > TETO_POR_LOTE) {
      aviso.textContent = `${pendentes} títulos — o limite por envio é `
        + `${TETO_POR_LOTE}. Estreite o filtro ou envie em partes.`;
    }
    aviso.classList.toggle("tem-pendencia", pendentes > 0);
    aviso.classList.toggle("passou-do-teto", pendentes > TETO_POR_LOTE);
    document.querySelectorAll("[data-precisa-pendencia]").forEach(
      b => { b.disabled = pendentes === 0; });
  }

  function marcados() {
    return [...new Set([...tabela.querySelectorAll("tbody input[data-marca]:checked")]
                       .map(c => c.closest("tr").dataset.codigo))];
  }

  // -------------------------------------------------------------------------
  // O seletor: um so para a tela inteira
  // -------------------------------------------------------------------------
  // Sao centenas de categorias. Desenhar a lista dentro de cada uma das ate
  // 3.000 linhas travaria o navegador — este painel ja morreu de memoria uma
  // vez. Existe UM seletor, que se move para perto de quem o chamou.
  function abrirPicker(campo, codigos, ancora) {
    if (!codigos.length) return;
    alvo = { campo, codigos };
    tituloPicker.textContent =
      (campo === "categoria" ? "Nova categoria" : "Nova obra (100% nela)") +
      (codigos.length > 1 ? ` — ${codigos.length} títulos` : "");

    const itens = campo === "categoria"
      ? CATEGORIAS.map(c => ({ codigo: c.codigo,
                               rotulo: `${c.descricao} (${c.codigo}) · ${c.onde}` }))
      : OBRAS.map(o => ({ codigo: o.codigo, rotulo: `${o.nome} (${o.codigo})` }));

    opcoesPicker.innerHTML = "";
    itens.forEach(i => {
      const b = document.createElement("button");
      b.type = "button";
      b.className = "picker-item";
      b.textContent = i.rotulo;
      b.dataset.codigo = i.codigo;
      b.dataset.texto = simples(i.rotulo);
      opcoesPicker.appendChild(b);
    });

    picker.classList.remove("oculto");
    const caixa = ancora.getBoundingClientRect();
    const altura = 330;
    // se nao couber embaixo, abre para cima — senao some fora da tela
    const paraBaixo = window.innerHeight - caixa.bottom > altura;
    picker.style.top = (paraBaixo ? caixa.bottom + 4
                                  : Math.max(8, caixa.top - altura - 4)) + "px";
    picker.style.left = Math.min(caixa.left, window.innerWidth - 380) + "px";
    buscaPicker.value = "";
    filtrarPicker();
    buscaPicker.focus();
  }

  function fecharPicker() { picker.classList.add("oculto"); alvo = null; }

  function filtrarPicker() {
    const termo = simples(buscaPicker.value);
    let quantos = 0;
    opcoesPicker.querySelectorAll(".picker-item").forEach(b => {
      const casa = !termo || b.dataset.texto.includes(termo);
      b.classList.toggle("oculto", !casa);
      if (casa) quantos++;
    });
    picker.querySelector("[data-picker-vazio]").classList.toggle("oculto", quantos > 0);
  }

  function escolher(codigo, rotulo) {
    if (!alvo) return;
    alvo.codigos.forEach(cod => {
      const atual = rascunhos.get(cod) || {};
      atual[alvo.campo] = { codigo, rotulo };
      rascunhos.set(cod, atual);
      pintar(cod);
    });
    fecharPicker();
    contar();
  }

  // -------------------------------------------------------------------------
  // Os cliques
  // -------------------------------------------------------------------------
  tabela.addEventListener("click", e => {
    const celula = e.target.closest("[data-campo]");
    if (celula) {
      abrirPicker(celula.dataset.campo, [celula.closest("tr").dataset.codigo], celula);
      return;
    }
    const desfazer = e.target.closest("[data-desfazer]");
    if (desfazer) {
      const codigo = desfazer.closest("tr").dataset.codigo;
      rascunhos.delete(codigo);
      pintar(codigo);
      contar();
    }
  });

  // marcar uma linha marca as outras linhas do MESMO titulo: no OMIE e um so
  tabela.addEventListener("change", e => {
    const marca = e.target.closest("input[data-marca]");
    if (!marca) return;
    linhasDe(marca.closest("tr").dataset.codigo).forEach(tr => {
      tr.querySelector("input[data-marca]").checked = marca.checked;
      tr.classList.toggle("marcada", marca.checked);
    });
    contar();
  });

  document.getElementById("marcar-todos").addEventListener("change", e => {
    tabela.querySelectorAll("tbody input[data-marca]").forEach(c => {
      if (c.closest("tr").classList.contains("oculto")) return;
      c.checked = e.target.checked;
      c.closest("tr").classList.toggle("marcada", e.target.checked);
    });
    contar();
  });

  document.querySelectorAll("[data-lote]").forEach(botao => {
    botao.addEventListener("click", () =>
      abrirPicker(botao.dataset.lote, marcados(), botao));
  });

  document.getElementById("limpar-rascunhos").addEventListener("click", () => {
    const codigos = [...rascunhos.keys()];
    rascunhos.clear();
    codigos.forEach(pintar);
    contar();
  });

  opcoesPicker.addEventListener("click", e => {
    const item = e.target.closest(".picker-item");
    if (item) escolher(item.dataset.codigo, item.textContent);
  });
  buscaPicker.addEventListener("input", filtrarPicker);
  buscaPicker.addEventListener("keydown", e => {
    if (e.key === "Escape") fecharPicker();
    if (e.key === "Enter") {
      e.preventDefault();
      const primeiro = opcoesPicker.querySelector(".picker-item:not(.oculto)");
      if (primeiro) escolher(primeiro.dataset.codigo, primeiro.textContent);
    }
  });
  picker.querySelector("[data-picker-fechar]").addEventListener("click", fecharPicker);
  document.addEventListener("click", e => {
    if (!picker.classList.contains("oculto") && !picker.contains(e.target)
        && !e.target.closest("[data-campo]") && !e.target.closest("[data-lote]")) {
      fecharPicker();
    }
  });

  // -------------------------------------------------------------------------
  // Enviar: o rascunho vira campos do formulario na hora do envio
  // -------------------------------------------------------------------------
  // Nao existe um campo escondido por linha. Com 3.000 linhas seriam 6.000
  // campos carregados a toa, e o que interessa e so o que foi rascunhado.
  form.addEventListener("submit", e => {
    const cofre = document.getElementById("campos-alteracao");
    cofre.innerHTML = "";
    if (rascunhos.size === 0) {
      e.preventDefault();
      alert("Nenhuma alteração pendente. Clique na Categoria ou na Obra de uma "
            + "linha para editar, ou marque várias e use os botões de cima.");
      return;
    }
    rascunhos.forEach((rascunho, codigo) => {
      const por = (nome, valor) => {
        const i = document.createElement("input");
        i.type = "hidden"; i.name = nome; i.value = valor;
        cofre.appendChild(i);
      };
      por("alvo_codigo", codigo);
      por("alvo_categoria", rascunho.categoria ? rascunho.categoria.codigo : "");
      por("alvo_departamento", rascunho.obra ? rascunho.obra.codigo : "");
    });
  });

  contar();
})();
