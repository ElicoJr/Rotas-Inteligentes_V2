param(
    [string]$Caminho = ".",
    [string]$ArquivoSaida = "estrutura.txt"
)

# Lista de nomes a serem ignorados
$Ignorar = @(".git", ".gitignore", ".gitattributes", ".github")

function Mapear-Diretorio {
    param(
        [string]$Diretorio,
        [int]$Nivel = 0
    )

    $prefixo = ("│   " * $Nivel) + "├── "

    try {
        $itens = Get-ChildItem -LiteralPath $Diretorio -Force | Sort-Object Name
    } catch {
        return
    }

    foreach ($item in $itens) {
        # pula itens da lista de ignorados
        if ($Ignorar -contains $item.Name) {
            continue
        }

        $linha = "$prefixo$($item.Name)"
        Write-Output $linha
        Add-Content -Path $ArquivoSaida -Value $linha

        if ($item.PSIsContainer) {
            Mapear-Diretorio -Diretorio $item.FullName -Nivel ($Nivel + 1)
        }
    }
}

# Limpa o arquivo anterior, se existir
if (Test-Path $ArquivoSaida) {
    Remove-Item $ArquivoSaida
}

Write-Host "📂 Mapeando estrutura a partir de: $((Resolve-Path $Caminho).Path)`n"

Mapear-Diretorio -Diretorio (Resolve-Path $Caminho).Path

Write-Host "`n✅ Estrutura salva em: $ArquivoSaida"

Read-Host -Prompt "Pressione qualquer tecla para sair..."