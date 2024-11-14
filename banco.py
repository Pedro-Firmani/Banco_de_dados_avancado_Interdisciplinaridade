import streamlit as st
import psycopg2
import pandas as pd
import psycopg2.errors
from psycopg2 import sql

# Função para conectar ao banco de dados PostgreSQL
def conectar_banco():
    try:
        conn = psycopg2.connect(
            dbname="Clientes",    # Substitua pelo nome do seu banco de dados
            user="postgres",      # Substitua pelo seu usuário
            password="postgres",  # Substitua pela sua senha
            host="localhost",     # Ou o host do seu servidor PostgreSQL
            port="5432"           # A porta padrão do PostgreSQL
        )
        conn.autocommit = False  # Desativa o autocommit
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para carregar todos os registros
def obter_dados():
    conn = conectar_banco()
    if conn is None:
        return pd.DataFrame()  # Retorna um DataFrame vazio se a conexão falhar
    try:
        query = "SELECT * FROM clientes ORDER BY id ASC"
        dados = pd.read_sql(query, conn)
        return dados
    except Exception as e:
        st.error(f"Erro ao consultar os dados: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# Função para obter registro específico
def obter_registro(id_cliente):
    conn = conectar_banco()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = "SELECT * FROM clientes WHERE id = %s"
            cur.execute(query, (id_cliente,))
            colunas = [desc[0] for desc in cur.description]
            dados = cur.fetchall()
            return pd.DataFrame(dados, columns=colunas)
    except Exception as e:
        st.error(f"Erro ao consultar o registro: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# Função para alterar cliente com logging detalhado
def alterar_cliente(conn, id_cliente, novo_nome, novo_limite, nome_atual, limite_atual):
    try:
        with conn.cursor() as cur:
            # Configura um timeout curto para evitar deadlocks prolongados
            cur.execute("SET LOCAL lock_timeout = '5s';")

            # Verifica os valores atuais no banco
            query_verificacao = "SELECT nome, limite FROM clientes WHERE id = %s"
            cur.execute(query_verificacao, (id_cliente,))
            resultado = cur.fetchone()

            if resultado != (nome_atual, limite_atual):
                mensagem_erro = (
                    f"Os dados do cliente estão desatualizados.\n"
                    f"**Dados no banco:** Nome: {resultado[0]}, Limite: {resultado[1]}\n"
                    f"**Dados na interface:** Nome: {nome_atual}, Limite: {limite_atual}"
                )
                conn.rollback()  # Certifique-se de fazer rollback para limpar transações pendentes
                return False, mensagem_erro

            # Atualiza o registro
            query_update = "UPDATE clientes SET nome = %s, limite = %s WHERE id = %s"
            cur.execute(query_update, (novo_nome, novo_limite, id_cliente))
            return True, "Alteração realizada. Confirme ou cancele a operação."
    except psycopg2.errors.DeadlockDetected:
        conn.rollback()  # Garante o rollback em caso de deadlock
        return False, "Deadlock detectado. Operação revertida. Atualize os dados antes de tentar novamente."
    except psycopg2.errors.LockNotAvailable:
        conn.rollback()  # Trata timeout de bloqueios
        return False, "Timeout ao tentar adquirir o bloqueio. Tente novamente mais tarde."
    except Exception as e:
        conn.rollback()  # Garante o rollback para outros erros
        return False, f"Erro durante a alteração dos dados: {e}"

# Inicializa o estado
if "id_selecionado" not in st.session_state:
    st.session_state.id_selecionado = None
if "novo_nome" not in st.session_state:
    st.session_state.novo_nome = ""
if "novo_limite" not in st.session_state:
    st.session_state.novo_limite = 0.0
if "conn_alteracao" not in st.session_state:
    st.session_state.conn_alteracao = None

# Interface Streamlit
st.title("Interface de Acesso ao Banco de Dados - PostgreSQL")

dados = obter_dados()
if not dados.empty:
    st.write("Dados da Tabela de Clientes:")
    st.dataframe(dados)

    id_cliente = st.text_input("Escolha o ID do cliente para edição:")
    if id_cliente:
        # Verifica se o ID mudou
        if id_cliente != st.session_state.id_selecionado:
            st.session_state.id_selecionado = id_cliente  # Atualiza o ID selecionado
            registro = obter_registro(id_cliente)
            if not registro.empty:
                st.session_state.novo_nome = registro.iloc[0]['nome']
                st.session_state.novo_limite = float(registro.iloc[0]['limite'])
            else:
                st.session_state.novo_nome = ""
                st.session_state.novo_limite = 0.0

        st.write(f"Cliente selecionado: {st.session_state.novo_nome}, Limite atual: {st.session_state.novo_limite}")

        novo_nome = st.text_input("Novo Nome:", value=st.session_state.novo_nome, key="novo_nome_input")
        novo_limite = st.number_input("Novo Limite:", value=st.session_state.novo_limite, step=1.0, key="novo_limite_input")

        if st.button("Alterar Cliente", key="alterar_cliente"):
            conn = conectar_banco()
            if conn:
                sucesso, mensagem = alterar_cliente(conn, id_cliente, novo_nome, novo_limite, st.session_state.novo_nome, st.session_state.novo_limite)
                st.session_state.conn_alteracao = conn  # Mantém a conexão aberta para commit ou rollback
                if sucesso:
                    st.success(mensagem)
                else:
                    st.error(mensagem)

        # Exibe os botões e a revisão dos dados SOMENTE se houver uma conexão aberta para alteração
        if st.session_state.conn_alteracao:
            st.write("### Revisão das Alterações")
            st.write("**Dados Antigos:**")
            st.write(f"Nome: {st.session_state.novo_nome}, Limite: {st.session_state.novo_limite}")
            st.write("**Dados Propostos:**")
            st.write(f"Novo Nome: {novo_nome}, Novo Limite: {novo_limite}")

            col1, col2 = st.columns(2)
            with col1:
                confirmar = st.button("Confirmar Alteração", key="confirmar_alteracao")
            with col2:
                cancelar = st.button("Cancelar Alteração", key="cancelar_alteracao")

            if confirmar:
                try:
                    st.session_state.conn_alteracao.commit()
                    st.success("Alteração confirmada e salva no banco de dados.")
                    # Limpa os campos
                    st.session_state.novo_nome = ""
                    st.session_state.novo_limite = 0.0
                    st.session_state.id_selecionado = None
                except Exception as e:
                    st.error(f"Erro ao salvar a alteração: {e}")
                finally:
                    st.session_state.conn_alteracao.close()
                    st.session_state.conn_alteracao = None

            if cancelar:
                try:
                    st.session_state.conn_alteracao.rollback()
                    st.warning("Alteração cancelada.")
                    # Limpa os campos
                    st.session_state.novo_nome = ""
                    st.session_state.novo_limite = 0.0
                    st.session_state.id_selecionado = None
                except Exception as e:
                    st.error(f"Erro ao cancelar a alteração: {e}")
                finally:
                    st.session_state.conn_alteracao.close()
                    st.session_state.conn_alteracao = None
else:
    st.error("Não foi possível carregar os dados. Verifique a conexão com o banco de dados.")
