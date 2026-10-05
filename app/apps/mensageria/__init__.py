# -*- coding: utf-8 -*-
"""
Mensageria — quem recebe o quê, por onde, com registro.

Pacote de código desde 05/10/2026 (antes só guardava a memória da área). O
que mora aqui:

  db.py      conexão com o banco do ERP, schema `mensageria`
  core.py    catálogo de tipos de mensagem, política por tipo, limite de
             WhatsApp, registro de envios — a DECISÃO de por onde sai
  gestao.py  a tela "Mensagens" dentro do ERP (pendurada no blueprint do ERP,
             como o ponto)

Quem ENVIA continua sendo o `app/apps/notificador.py`: ele pergunta aqui
("este tipo sai por onde? ainda cabe no limite?"), manda, e registra aqui.
Não importar este pacote no `app/main.py`: ele não é blueprint.
"""
