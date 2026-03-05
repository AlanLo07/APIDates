# Versión para PowerShell
sam build
sam deploy `
    --stack-name planes-crud-stack `
    --region us-east-2 `
    --capabilities CAPABILITY_IAM `
    --resolve-s3 `
    --no-confirm-changeset

# Obtener la URL
aws cloudformation describe-stacks `
    --stack-name planes-crud-stack `
    --query "Stacks[0].Outputs[?OutputKey=='FunctionUrl'].OutputValue" `
    --output text