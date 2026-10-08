/* Campos de MÊS e de PERÍODO do painel — digitar OU clicar e escolher.
 *
 * Pedido do dono, 07/10/2026:
 *   - mês: "estamos no Brasil, tem que ser MM/AAAA" e "não tem como abrir uma
 *     espécie de calendário? Clica, abre. Ou escrever 01/… — não um calendário
 *     completo, porque eu só preciso informar a competência";
 *   - período: "no calendário a gente só visualiza um mês, não tem como colocar
 *     início e fim".
 *
 * Como funciona, para toda tela do painel, sem cada tela precisar lembrar:
 *   - todo <input type="month"> vira um campo "MM/AAAA" que aceita digitação
 *     (a barra entra sozinha) e, ao clicar, abre os 12 meses com o ano em cima;
 *   - todo par de <input type="date"> "de"/"até" vira dois campos "dd/mm/aaaa"
 *     que aceitam digitação e, ao clicar em qualquer um, abrem UM calendário de
 *     dois meses lado a lado: o primeiro clique é o início, o segundo o fim;
 *   - a data sozinha (sem par) ganha o mesmo calendário, com um clique só.
 *
 * O campo original continua na página, escondido, com o MESMO nome e o valor
 * no formato de sempre (AAAA-MM / AAAA-MM-DD): o servidor e os scripts das
 * telas não percebem a troca. Quem não quiser marca o campo com data-sem-troca.
 */
(function () {
  "use strict";
  const MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
                 "agosto", "setembro", "outubro", "novembro", "dezembro"];
  const CURTOS = ["jan", "fev", "mar", "abr", "mai", "jun",
                  "jul", "ago", "set", "out", "nov", "dez"];
  const SEMANA = ["D", "S", "T", "Q", "Q", "S", "S"];
  const nativo = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value");
  const d2 = n => String(n).padStart(2, "0");
  const iso = d => `${d.getFullYear()}-${d2(d.getMonth() + 1)}-${d2(d.getDate())}`;
  const deIso = v => {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(v || "");
    if (!m) return null;
    const d = new Date(+m[1], +m[2] - 1, +m[3]);
    return d.getMonth() === +m[2] - 1 ? d : null;
  };
  const br = v => { const d = deIso(v); return d ? `${d2(d.getDate())}/${d2(d.getMonth() + 1)}/${d.getFullYear()}` : ""; };

  // ---------------------------------------------------------------- comum
  let aberto = null;              // {pop, ancora, fechar}
  function fecharAberto() {
    if (aberto) { aberto.pop.remove(); aberto = null; }
  }
  document.addEventListener("mousedown", e => {
    if (aberto && !aberto.pop.contains(e.target) && !aberto.ancoras.some(a => a.contains(e.target)))
      fecharAberto();
  });
  document.addEventListener("keydown", e => { if (e.key === "Escape") fecharAberto(); });
  window.addEventListener("resize", fecharAberto);

  function abrirPopup(ancora, ancoras, montar) {
    fecharAberto();
    const pop = document.createElement("div");
    pop.className = "seletor-pop";
    document.body.appendChild(pop);
    aberto = {pop, ancoras};
    montar(pop);
    const r = ancora.getBoundingClientRect();
    const largura = pop.offsetWidth;
    let esquerda = window.scrollX + r.left;
    const limite = window.scrollX + document.documentElement.clientWidth - largura - 8;
    esquerda = Math.max(window.scrollX + 8, Math.min(esquerda, limite));
    pop.style.left = esquerda + "px";
    pop.style.top = (window.scrollY + r.bottom + 4) + "px";
    return pop;
  }

  // O campo original fica escondido; o valor dele, escrito por script, faz o
  // campo visível acompanhar.
  function esconderOriginal(campo, aoMudar) {
    campo.type = "hidden";
    Object.defineProperty(campo, "value", {
      configurable: true,
      get: () => nativo.get.call(campo),
      set: v => { nativo.set.call(campo, v); aoMudar(v); },
    });
  }
  function gravar(campo, v) {
    if (v === nativo.get.call(campo)) return;
    nativo.set.call(campo, v);
    campo.dispatchEvent(new Event("input", {bubbles: true}));
    campo.dispatchEvent(new Event("change", {bubbles: true}));
  }
  // sair do campo com o teclado (Tab) fecha a janelinha
  function fecharAoSair(t) {
    t.addEventListener("blur", () => setTimeout(() => {
      if (aberto && !aberto.ancoras.includes(document.activeElement)) fecharAberto();
    }, 150));
  }

  function campoVisivel(original, placeholder, tamanho) {
    const t = document.createElement("input");
    t.type = "text";
    t.inputMode = "numeric";
    t.autocomplete = "off";
    t.placeholder = placeholder;
    t.maxLength = tamanho;
    t.className = "campo-data-texto";
    t.setAttribute("data-sem-mascara", "");
    if (original.id) { t.id = original.id + "-texto"; }
    const rotulo = original.id && document.querySelector(`label[for="${original.id}"]`);
    if (rotulo) rotulo.htmlFor = t.id;
    if (original.getAttribute("aria-label")) t.setAttribute("aria-label", original.getAttribute("aria-label"));
    if (original.style.width) t.style.width = original.style.width;
    original.parentNode.insertBefore(t, original);
    fecharAoSair(t);
    return t;
  }
  // digitos com as barras nos lugares: [2] -> "MM/AAAA", [2, 5] -> "dd/mm/aaaa"
  function mascarar(texto, barras) {
    const dig = texto.replace(/\D/g, "");
    let saida = "", i = 0;
    for (const ch of dig) {
      if (barras.includes(saida.length)) saida += "/";
      saida += ch;
      if (++i >= (barras.length === 1 ? 6 : 8)) break;
    }
    return saida;
  }

  // ---------------------------------------------------------------- MÊS
  function trocarMes(campo) {
    const t = campoVisivel(campo, "MM/AAAA", 7);
    const mostrar = v => {
      const m = /^(\d{4})-(\d{2})$/.exec(v || "");
      t.value = m ? `${m[2]}/${m[1]}` : "";
    };
    esconderOriginal(campo, mostrar);
    mostrar(nativo.get.call(campo));

    t.addEventListener("input", () => {
      t.value = mascarar(t.value, [2]);
      const m = /^(\d{2})\/(\d{4})$/.exec(t.value);
      if (m && +m[1] >= 1 && +m[1] <= 12) gravar(campo, `${m[2]}-${m[1]}`);
      else if (!t.value) gravar(campo, "");
    });
    t.addEventListener("blur", () => mostrar(nativo.get.call(campo)));
    t.addEventListener("keydown", e => {
      if (e.key === "Enter" && aberto) { e.preventDefault(); fecharAberto(); }
    });

    const abrir = () => {
      const atual = /^(\d{4})-(\d{2})$/.exec(nativo.get.call(campo));
      const hoje = new Date();
      let ano = atual ? +atual[1] : hoje.getFullYear();
      abrirPopup(t, [t], pop => {
        const desenhar = () => {
          pop.innerHTML = "";
          const cab = document.createElement("div");
          cab.className = "seletor-cab";
          const ant = botao("‹", "ano anterior", () => { ano--; desenhar(); });
          const titulo = document.createElement("b");
          titulo.textContent = ano;
          const seg = botao("›", "ano seguinte", () => { ano++; desenhar(); });
          cab.append(ant, titulo, seg);
          const grade = document.createElement("div");
          grade.className = "seletor-meses";
          CURTOS.forEach((nome, i) => {
            const b = botao(nome, `${MESES[i]} de ${ano}`, () => {
              gravar(campo, `${ano}-${d2(i + 1)}`);
              mostrar(nativo.get.call(campo));
              fecharAberto();
            });
            if (atual && +atual[1] === ano && +atual[2] === i + 1) b.classList.add("escolhido");
            if (hoje.getFullYear() === ano && hoje.getMonth() === i) b.classList.add("hoje");
            grade.appendChild(b);
          });
          pop.append(cab, grade);
        };
        desenhar();
      });
    };
    t.addEventListener("click", abrir);
    t.addEventListener("focus", abrir);
  }

  function botao(texto, titulo, acao) {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = texto;
    if (titulo) b.title = titulo;
    b.addEventListener("mousedown", e => e.preventDefault());   // não tira o foco
    b.addEventListener("click", e => { e.preventDefault(); acao(); });
    return b;
  }

  // ---------------------------------------------------------------- DATA / PERÍODO
  // `par`: {inicio, fim} (campos originais) ou só {inicio} para data sozinha
  function trocarDatas(par) {
    const campos = par.fim ? [par.inicio, par.fim] : [par.inicio];
    const textos = campos.map(c => campoVisivel(c, "dd/mm/aaaa", 10));
    const mostrar = () => campos.forEach((c, i) => { textos[i].value = br(nativo.get.call(c)); });
    campos.forEach(c => esconderOriginal(c, mostrar));
    mostrar();

    campos.forEach((c, i) => {
      const t = textos[i];
      t.addEventListener("input", () => {
        t.value = mascarar(t.value, [2, 5]);
        const m = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(t.value);
        if (m) {
          const v = `${m[3]}-${m[2]}-${m[1]}`;
          if (deIso(v)) gravar(c, v);
        } else if (!t.value) gravar(c, "");
      });
      t.addEventListener("blur", mostrar);
      t.addEventListener("click", () => abrir(i));
      t.addEventListener("focus", () => abrir(i));
      t.addEventListener("keydown", e => {
        if (e.key === "Enter" && aberto) { e.preventDefault(); fecharAberto(); }
      });
    });

    function abrir(quem) {
      if (aberto && aberto.dono === par) return;
      const hoje = new Date();
      let ini = deIso(nativo.get.call(campos[0]));
      let fim = par.fim ? deIso(nativo.get.call(campos[1])) : null;
      // o primeiro clique no calendário começa um período novo
      let esperandoFim = false;
      let foco = (quem === 1 && fim) ? fim : (ini || hoje);
      let mesVisto = new Date(foco.getFullYear(), foco.getMonth(), 1);
      if (par.fim && quem === 1 && fim) mesVisto = new Date(fim.getFullYear(), fim.getMonth() - 1, 1);
      let passando = null;
      const duasColunas = par.fim && document.documentElement.clientWidth >= 560;

      const pop = abrirPopup(textos[quem], textos, pop => {
        let dias = [];
        const pintar = () => {
          const fimVisto = esperandoFim ? (passando || ini) : fim;
          const [a, b] = (ini && fimVisto && fimVisto < ini) ? [fimVisto, ini] : [ini, fimVisto];
          for (const [d, bt] of dias) {
            const v = iso(d);
            bt.classList.toggle("escolhido", !!((a && v === iso(a)) || (b && v === iso(b))));
            bt.classList.toggle("no-periodo", !!(a && b && d > a && d < b));
          }
        };
        const desenhar = () => {
          pop.innerHTML = "";
          dias = [];
          const cab = document.createElement("div");
          cab.className = "seletor-cab";
          const mover = (meses) => () => {
            mesVisto = new Date(mesVisto.getFullYear(), mesVisto.getMonth() + meses, 1); desenhar();
          };
          cab.append(botao("«", "ano anterior", mover(-12)), botao("‹", "mês anterior", mover(-1)));
          const titulo = document.createElement("span");
          titulo.className = "seletor-dica";
          titulo.textContent = !par.fim ? "escolha o dia"
            : (esperandoFim ? "agora clique no fim" : "clique no início e depois no fim");
          cab.append(titulo, botao("›", "mês seguinte", mover(1)), botao("»", "ano seguinte", mover(12)));
          pop.appendChild(cab);
          const meses = document.createElement("div");
          meses.className = "seletor-dois-meses";
          for (let k = 0; k < (duasColunas ? 2 : 1); k++)
            meses.appendChild(grade(new Date(mesVisto.getFullYear(), mesVisto.getMonth() + k, 1)));
          pop.appendChild(meses);
          pintar();
          if (par.fim) {
            const atalhos = document.createElement("div");
            atalhos.className = "seletor-atalhos";
            const m0 = new Date(hoje.getFullYear(), hoje.getMonth(), 1);
            const fimDoMes = d => new Date(d.getFullYear(), d.getMonth() + 1, 0);
            const m1 = new Date(hoje.getFullYear(), hoje.getMonth() - 1, 1);
            atalhos.append(
              botao("Hoje", "", () => escolherPeriodo(hoje, hoje)),
              botao("Este mês", "", () => escolherPeriodo(m0, fimDoMes(m0))),
              botao("Mês passado", "", () => escolherPeriodo(m1, fimDoMes(m1))),
              botao("Este ano", "", () => escolherPeriodo(new Date(hoje.getFullYear(), 0, 1),
                                                         new Date(hoje.getFullYear(), 11, 31))),
              botao("Limpar", "", () => escolherPeriodo(null, null)));
            pop.appendChild(atalhos);
          }
        };
        const grade = (primeiro) => {
          const caixa = document.createElement("div");
          caixa.className = "seletor-mes";
          const nome = document.createElement("div");
          nome.className = "seletor-nome-mes";
          nome.textContent = `${MESES[primeiro.getMonth()]} de ${primeiro.getFullYear()}`;
          caixa.appendChild(nome);
          const g = document.createElement("div");
          g.className = "seletor-dias";
          SEMANA.forEach(s => { const c = document.createElement("span"); c.textContent = s; c.className = "seletor-semana"; g.appendChild(c); });
          for (let i = 0; i < primeiro.getDay(); i++) g.appendChild(document.createElement("span"));
          const ultimo = new Date(primeiro.getFullYear(), primeiro.getMonth() + 1, 0).getDate();
          for (let dia = 1; dia <= ultimo; dia++) {
            const d = new Date(primeiro.getFullYear(), primeiro.getMonth(), dia);
            const bt = botao(String(dia), "", () => clicar(d));
            if (iso(d) === iso(hoje)) bt.classList.add("hoje");
            // passar o mouse mostra o período que o clique vai marcar — só
            // repinta, não redesenha (redesenhar sob o mouse perdia o clique)
            bt.addEventListener("mouseenter", () => { if (esperandoFim) { passando = d; pintar(); } });
            dias.push([d, bt]);
            g.appendChild(bt);
          }
          caixa.appendChild(g);
          return caixa;
        };
        const clicar = d => {
          if (!par.fim) { gravar(campos[0], iso(d)); mostrar(); fecharAberto(); return; }
          if (!esperandoFim) {
            ini = d; fim = null; esperandoFim = true; passando = null; desenhar(); return;
          }
          escolherPeriodo(ini, d);
        };
        const escolherPeriodo = (x, y) => {
          if (x && y && y < x) [x, y] = [y, x];
          gravar(campos[0], x ? iso(x) : "");
          gravar(campos[1], y ? iso(y) : "");
          mostrar();
          fecharAberto();
        };
        desenhar();
      });
      aberto.dono = par;
      return pop;
    }
  }

  // ---------------------------------------------------------------- ligar
  function ligar() {
    document.querySelectorAll('input[type="month"]:not([data-sem-troca])').forEach(trocarMes);
    const datas = [...document.querySelectorAll('input[type="date"]:not([data-sem-troca])')];
    const nome = c => (c.id || c.name || "").toLowerCase();
    for (let i = 0; i < datas.length; i++) {
      const c = datas[i], prox = datas[i + 1];
      if (prox && /(^|[-_])de$/.test(nome(c)) && /(^|[-_])ate$/.test(nome(prox))) {
        trocarDatas({inicio: c, fim: prox});
        i++;
      } else {
        trocarDatas({inicio: c});
      }
    }
  }
  ligar();
})();
