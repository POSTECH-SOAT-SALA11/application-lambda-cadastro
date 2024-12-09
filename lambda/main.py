import json
import boto3
import psycopg2
from datetime import datetime

# Configurações de conexão com o Aurora PostgreSQL
DB_HOST = 'avalanches-pedido-db.c9a4qi2g0wqh.sa-east-1.rds.amazonaws.com'
DB_NAME = 'avalanches_pedido_db'
DB_USER = 'dbadminuser'
DB_PASSWORD = 'G5g!0xB&O5P2TWzJ'
DB_PORT = 5432

def lambda_handler(event, context):
    client = boto3.client('cognito-idp')

    # Identificar o ID enviado no header
    user_id = event['headers'].get('id')
    if not user_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "ID (CPF) não fornecido no header."})
        }

    try:
        user_id = user_id.strip()
        # Verificar se é um CPF
        if is_valid_cpf(user_id):
            user_pool_id = 'sa-east-1_qncAjoEa8'
            user_attributes = [{"Name": "preferred_username", "Value": user_id}]
            result = create_user(client, user_pool_id, user_attributes)

            # Usar valores padrão para nome e email
            nome = 'Usuário Desconhecido'
            email = f"{user_id}@exemplo.com"  # Gerar email fictício baseado no CPF

            insert_into_database(nome, user_id, email)

            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Usuário cliente cadastrado com sucesso.", "result": sanitize_response(result)})
            }
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "ID inválido. Não é um CPF válido."})
            }
    except client.exceptions.UsernameExistsException:
        return {
            "statusCode": 409,
            "body": json.dumps({"message": "Usuário já existe no pool especificado."})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Erro ao cadastrar usuário: {str(e)}"})
        }

def is_valid_cpf(cpf):
    """
    Valida se o CPF fornecido é válido.
    """
    return len(cpf) == 11 and cpf.isdigit()

def create_user(client, user_pool_id, user_attributes):
    """
    Cria um usuário no Cognito User Pool especificado.
    """
    response = client.admin_create_user(
        UserPoolId=user_pool_id,
        Username=user_attributes[0]['Value'],
        UserAttributes=user_attributes,
        MessageAction='SUPPRESS'  # Evita o envio de email automático
    )
    return response

def insert_into_database(nome, cpf, email):
    """
    Insere os dados do cliente na tabela do PostgreSQL.
    """
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Inserir dados na tabela cliente
        insert_query = """
            INSERT INTO public.cliente (nome, cpf, email)
            VALUES (%s, %s, %s);
        """
        cursor.execute(insert_query, (nome, cpf, email))

        # Commit e fechamento da conexão
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        raise Exception(f"Erro ao inserir no banco de dados: {str(e)}")

def sanitize_response(response):
    """
    Remove ou converte valores não serializáveis (como datetime) do response.
    """
    def default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    return json.loads(json.dumps(response, default=default_serializer))
