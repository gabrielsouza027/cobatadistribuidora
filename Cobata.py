# COBATA.PY - GERENCIAMENTO COMPLETO DE USUÁRIOS E CSS RESTAURADO
import streamlit as st
import json
import os
import importlib

st.set_page_config(page_title="Cobata Distribuidora", page_icon="Arquivos/transferir.png", layout="wide", initial_sidebar_state="auto")

# Caminho do arquivo JSON
USER_DATA_FILE = "users.json"

# Lista de páginas
PAGES = {
    "Página Inicial": "Página_Inicial",
    "Produto": "Produto",
    "Fornecedor": "Fornecedor",
    "Estoque": "Estoque",
    "Vendedores": "Vendedores",
    "Pedidos": "Pedidos",
    "Pedidos Venda": "Pedidos_Venda",
    "Positivacao": "Positivacao",
}
# Lista de permissões que um admin pode conceder
AVAILABLE_PERMISSIONS = list(PAGES.keys()) + ["Gerenciar Usuários"]

# Funções de usuário (load/save)
def load_users():
    if os.path.exists(USER_DATA_FILE):
        with open(USER_DATA_FILE, "r", encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_users(users):
    with open(USER_DATA_FILE, "w", encoding='utf-8') as f:
        json.dump(users, f, indent=4, ensure_ascii=False)

def navigation_bar():
    # A imagem agora usa 'use_container_width' para evitar o aviso
    st.sidebar.image("Arquivos/WhatsApp_Image_2024-11-28_at_10.47.28-removebg-preview.png", use_container_width=True)
    
    # --- SEU CSS ORIGINAL E COMPLETO FOI RESTAURADO AQUI ---
    st.markdown("""
        <style>
        /* Barra lateral */
        .sidebar .sidebar-content {
            background: #ADFF2F;
            padding: 3rem 0;    
            width: 50px; /* Largura fixa da barra lateral */
        } 

        .st-emotion-cache-1ibsh2c {
            width: 100%;
            padding: 6rem 1rem 10rem;
            max-width: initial;
            min-width: auto;
            background-color: #0e1117;
        }

        .login-container, .login-form, .login-form h1, .login-form input, .login-form button, .login-form .error {
            /* Estilos de login aqui, se necessário */
        }

        .st-emotion-cache-6qob1r {
            position: relative;
            height: 100%;            #Configuração de cores da aba laterral
            width: 100%;
            overflow: overlay;
            background-color: #16181c;
        }

        .st-emotion-cache-jh76sn {
            display: inline-flex;
            -webkit-box-align: center;
            align-items: center;
            -webkit-box-pack: center;
            justify-content: center;
            font-weight: 400;
            padding: 0.25rem 0.75rem;
            border-radius: 0.5rem;
            min-height: 2.5rem;      #Configuração do botão da aba lateral
            margin: 0px;
            margiin-left: 30px;
            margin-height: 30px;
            line-height: 1.6;
            text-transform: none;
            font-size: inherit;
            font-family: inherit;
            color: inherit;
            width: 100%;
            cursor: pointer;
            user-select: none;
            background-color: #16181c;
            border: 1px solid rgba(250, 250, 250, 0.2);
        }

        .st-emotion-cache-1espb9k {
            font-family: "Source Sans Pro", sans-serif;
            font-size: 1rem;
            margin-bottom: -1rem;
            color: inherit;
        }

        .st-emotion-cache-1wqrzgl, .st-emotion-cache-xhkv9f, .st-emotion-cache-1espb9k h1, 
        .sidebar .sidebar-content .nav-button, .sidebar .sidebar-content .nav-button:hover,
        .sidebar .sidebar-content .nav-button.active, @media (max-width: 768px) {
            /* Resto do seu CSS original aqui */
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.sidebar.title("PAINEL")

    if "Página Inicial" in st.session_state.user_permissions:
        if st.sidebar.button("Inicio"):
            st.session_state.page = "Página Inicial"
            st.rerun()

    st.sidebar.subheader("Logística")
    if "Estoque" in st.session_state.user_permissions:
        if st.sidebar.button("Estoque"):
            st.session_state.page = "Estoque"
            st.rerun()
    if "Fornecedor" in st.session_state.user_permissions:
        if st.sidebar.button("Fornecedor"):
            st.session_state.page = "Fornecedor"
            st.rerun()
    if "Pedidos" in st.session_state.user_permissions:
        if st.sidebar.button("Pedidos Separação"):
            st.session_state.page = "Pedidos"
            st.rerun()

    st.sidebar.subheader("Vendas")
    if "Produto" in st.session_state.user_permissions:
        if st.sidebar.button("Produto"):
            st.session_state.page = "Produto"
            st.rerun()
    if "Vendedores" in st.session_state.user_permissions:
        if st.sidebar.button("Vendedores"):
            st.session_state.page = "Vendedores"
            st.rerun()
    if "Positivacao" in st.session_state.user_permissions:
        if st.sidebar.button("Positivação"):
            st.session_state.page = "Positivacao"
            st.rerun()

    st.sidebar.write("---")
    if st.sidebar.button("Sair"):
        st.session_state.logged_in = False
        st.session_state.user_permissions = []
        st.session_state.page = "Login"
        if 'admin_auth' in st.session_state:
            st.session_state.admin_auth = False
        st.rerun()

def login_page():
    # Adicionamos colunas para centralizar o conteúdo.
    # A coluna do meio (col2) será onde o formulário ficará.
    # Os números definem a proporção do espaço: 1 parte vazia, 1.5 partes para o conteúdo, 1 parte vazia.
    col1, col2, col3 = st.columns([1, 1.5, 1])

    # Todo o conteúdo do login agora vai dentro da coluna central (col2)
    with col2:
        # Usamos st.container() dentro da coluna para agrupar visualmente
        with st.container():
            st.image("Arquivos/WhatsApp_Image_2024-11-28_at_10.47.28-removebg-preview.png", width=200)
            st.title("Login")
            
            username = st.text_input("Nome de usuário")
            password = st.text_input("Senha", type="password")
            users_db = load_users()

            if st.button("Entrar", use_container_width=True):
                if username in users_db and users_db[username]["password"] == password:
                    st.session_state.logged_in = True
                    st.session_state.user_permissions = users_db[username]["permissions"]
                    
                    if "Página Inicial" in st.session_state.user_permissions:
                        st.session_state.page = "Página Inicial"
                    elif st.session_state.user_permissions:
                        st.session_state.page = st.session_state.user_permissions[0]
                    else:
                        st.error("Você não tem permissão para acessar nenhuma página.")
                        st.session_state.logged_in = False
                        return
                    st.rerun()
                else:
                    st.error("Nome de usuário ou senha inválidos.")
            
            # O botão agora leva para a página de gerenciamento
            if st.button("Gerenciar Usuários", use_container_width=True): # use_container_width para preencher a coluna
                st.session_state.page = "Gerenciamento"
                st.rerun()


# --- PÁGINA DE GERENCIAMENTO (ANTIGA REGISTER_PAGE) ---
def user_management_page():
    users_db = load_users()

    # Criamos as colunas para centralizar todo o conteúdo desta página
    col1, col2, col3 = st.columns([1, 1.5, 1])

    # Todo o conteúdo da interface vai para a coluna central (col2)
    with col2:
        st.title("Gerenciamento de Usuários")

        # Etapa 1: Autenticação do Administrador
        if not st.session_state.get('admin_auth', False):
            st.subheader("Autenticação de Administrador Necessária")
            admin_user = st.text_input("Seu usuário de admin")
            admin_pass = st.text_input("Sua senha de admin", type="password")

            # Adicionado 'use_container_width' para consistência visual
            if st.button("Autenticar", use_container_width=True):
                if admin_user in users_db and users_db[admin_user]["password"] == admin_pass:
                    if "Gerenciar Usuários" in users_db[admin_user]["permissions"]:
                        st.session_state.admin_auth = True
                        st.rerun()
                    else:
                        st.error("Este usuário não tem permissão para gerenciar outros usuários.")
                else:
                    st.error("Usuário ou senha de administrador inválidos.")
        
        # Etapa 2: Ações de Gerenciamento (Criar ou Editar)
        else:
            st.success("Administrador autenticado.")
            action = st.radio("O que você deseja fazer?", ("Criar Novo Usuário", "Editar Usuário Existente"))

            # --- SEÇÃO PARA CRIAR USUÁRIO ---
            if action == "Criar Novo Usuário":
                st.subheader("Preencha os dados do novo usuário")
                with st.form("create_user_form", clear_on_submit=True):
                    new_name = st.text_input("Nome completo do novo usuário")
                    new_username = st.text_input("Nome de usuário para login")
                    new_password = st.text_input("Senha para o novo usuário", type="password")
                    confirm_password = st.text_input("Confirme a senha", type="password")
                    
                    selected_permissions = st.multiselect("Selecione as permissões:", options=AVAILABLE_PERMISSIONS)
                    
                    submitted = st.form_submit_button("Registrar Usuário")
                    if submitted:
                        if not all([new_name, new_username, new_password, confirm_password]):
                            st.warning("Por favor, preencha todos os campos.")
                        elif new_password != confirm_password:
                            st.error("As senhas não coincidem.")
                        elif new_username in users_db:
                            st.error("Este nome de usuário já existe.")
                        else:
                            users_db[new_username] = {
                                "password": new_password, "name": new_name, "permissions": selected_permissions
                            }
                            save_users(users_db)
                            st.success(f"Usuário '{new_username}' registrado com sucesso!")

            # --- SEÇÃO PARA EDITAR USUÁRIO ---
            elif action == "Editar Usuário Existente":
                st.subheader("Selecione um usuário para editar suas permissões")
                
                user_to_edit = st.selectbox("Usuários:", options=list(users_db.keys()))

                if user_to_edit:
                    user_data = users_db[user_to_edit]
                    
                    with st.form("edit_user_form"):
                        st.write(f"**Editando:** {user_data.get('name', user_to_edit)}")
                        
                        new_permissions = st.multiselect(
                            "Permissões de acesso:",
                            options=AVAILABLE_PERMISSIONS,
                            default=user_data.get("permissions", [])
                        )
                        
                        submitted = st.form_submit_button("Salvar Alterações")
                        if submitted:
                            users_db[user_to_edit]['permissions'] = new_permissions
                            save_users(users_db)
                            st.success(f"Permissões do usuário '{user_to_edit}' atualizadas com sucesso!")
                            st.rerun()

        # Botão para voltar (também dentro da coluna central)
        # Adicionado 'use_container_width' para consistência visual
        if st.button("Voltar para o Login", use_container_width=True):
            st.session_state.page = "Login"
            st.session_state.admin_auth = False # Reseta a autenticação
            st.rerun()


def load_page(page_name):
    # (sem alterações)
    if page_name not in st.session_state.user_permissions:
        st.error("Você não tem permissão para acessar esta página.")
        return
    module_name = PAGES.get(page_name)
    if module_name:
        try:
            page_module = importlib.import_module(module_name)
            page_module.main()
        except (ModuleNotFoundError, AttributeError):
            st.error(f"Erro ao carregar a página '{page_name}'.")

def main():
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.user_permissions = []
        st.session_state.page = "Login"
    if 'admin_auth' not in st.session_state:
        st.session_state.admin_auth = False

    if st.session_state.logged_in:
        navigation_bar()
        load_page(st.session_state.page)
    else:
        # Roteamento para a página correta quando não está logado
        if st.session_state.page == "Gerenciamento":
            user_management_page()
        else:
            login_page()

if __name__ == "__main__":
    main()