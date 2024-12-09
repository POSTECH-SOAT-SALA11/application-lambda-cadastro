import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client = boto3.client('cognito-idp')

    # Identificar o ID enviado no header
    user_id = event['headers'].get('id')
    if not user_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "ID (CPF ou Matrícula) não fornecido no header."})
        }

    try:
        user_id = user_id.strip()
        # Verificar se é um CPF
        if is_valid_cpf(user_id):
            user_pool_id = 'sa-east-1_qncAjoEa8'
            user_attributes = [{"Name": "preferred_username", "Value": user_id}]
            result = create_user(client, user_pool_id, user_attributes)
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Usuário cliente cadastrado com sucesso.", "result": sanitize_response(result)})
            }
        # Verificar se é uma matrícula
        elif is_valid_matricula(user_id):
            user_pool_id = 'sa-east-1_Jwqyi5DHG'
            user_attributes = [{"Name": "preferred_username", "Value": user_id}]
            result = create_user(client, user_pool_id, user_attributes)
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Funcionário cadastrado com sucesso.", "result": sanitize_response(result)})
            }
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "ID inválido. Não é um CPF ou Matrícula válida."})
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


def is_valid_matricula(matricula):
    """
    Valida se a matrícula fornecida é válida.
    """
    return matricula.startswith('RM') and len(matricula) == 8 and matricula[2:].isdigit()


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


def sanitize_response(response):
    """
    Remove ou converte valores não serializáveis (como datetime) do response.
    """
    def default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    return json.loads(json.dumps(response, default=default_serializer))