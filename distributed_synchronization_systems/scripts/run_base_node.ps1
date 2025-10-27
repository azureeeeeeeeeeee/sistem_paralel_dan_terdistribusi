param(
    [Parameter(Mandatory=$true)]
    [int]$port
)

cd ..

./venv/scripts/activate

python -m src.nodes.base_node --port $port