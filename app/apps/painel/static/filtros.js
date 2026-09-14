// ---------------------------------------------------------------------------
// Busca dentro do filtro — vale para TODA barra lateral do painel
// ---------------------------------------------------------------------------
// Tudo acontece no navegador: a lista inteira ja veio do servidor, entao
// buscar nao recarrega a pagina nem perde o que ja estava marcado.
//
// Estava embutido no `painel_filtros.html` ate 13/09/2026. Saiu de la quando
// o Explorador ganhou a sua propria barra: duas copias divergiriam na
// primeira correcao.
(function () {
  // "Fortaleza" tem de ser achado digitando "fortaleza"; "SÃO" digitando "sao".
  // Sem tirar o acento, quem digita rápido não acha nada e acha que quebrou.
  const simples = t => (t || "").normalize("NFD")
                                .replace(/[̀-ͯ]/g, "")
                                .toLowerCase().trim();

  document.querySelectorAll(".filtro[data-filtro]").forEach(bloco => {
    const busca = bloco.querySelector("[data-busca]");
    const opcoes = bloco.querySelector("[data-opcoes]");
    const contador = bloco.querySelector("[data-contador]");
    const visiveis = bloco.querySelector("[data-visiveis]");
    const vazio = bloco.querySelector("[data-vazio]");
    const linhas = Array.from(opcoes.querySelectorAll(".opcao"));

    function atualizarContador() {
      const marcados = linhas.filter(l => l.querySelector("input").checked).length;
      contador.textContent = marcados;
      contador.classList.toggle("oculto", marcados === 0);
    }

    // O que está marcado sobe para o topo: procurando a décima obra, as nove
    // já escolhidas continuam à vista, e dá para conferir sem rolar de volta.
    function reordenar() {
      const marcados = linhas.filter(l => l.querySelector("input").checked);
      const resto = linhas.filter(l => !l.querySelector("input").checked);
      [...marcados, ...resto].forEach(l => opcoes.appendChild(l));
    }

    function filtrar() {
      const termo = simples(busca ? busca.value : "");
      let mostrando = 0;
      linhas.forEach(linha => {
        const marcado = linha.querySelector("input").checked;
        // o que já está marcado nunca some da lista, mesmo fora da busca
        const casa = !termo || marcado || simples(linha.dataset.texto).includes(termo);
        linha.classList.toggle("oculto", !casa);
        if (casa) mostrando++;
      });
      if (visiveis) {
        visiveis.textContent = termo ? `${mostrando} de ${linhas.length}`
                                     : `${linhas.length} itens`;
      }
      if (vazio) vazio.classList.toggle("oculto", mostrando > 0);
    }

    opcoes.addEventListener("change", () => { atualizarContador(); });

    if (busca) {
      busca.addEventListener("input", filtrar);
      // Enter dentro da busca filtra, não envia o formulário meio preenchido
      busca.addEventListener("keydown", e => {
        if (e.key === "Enter") { e.preventDefault(); filtrar(); }
        if (e.key === "Escape") { busca.value = ""; filtrar(); }
      });
      const marcarVisiveis = bloco.querySelector("[data-marcar-visiveis]");
      const desmarcar = bloco.querySelector("[data-desmarcar]");
      marcarVisiveis.onclick = () => {
        linhas.filter(l => !l.classList.contains("oculto"))
              .forEach(l => { l.querySelector("input").checked = true; });
        atualizarContador(); reordenar(); filtrar();
      };
      desmarcar.onclick = () => {
        linhas.forEach(l => { l.querySelector("input").checked = false; });
        atualizarContador(); filtrar();
      };
    }

    reordenar();
    atualizarContador();
    filtrar();
  });
})();
