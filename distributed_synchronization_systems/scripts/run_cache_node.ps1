param(
    [Parameter(Mandatory=$true)]
    [int]$port
)

cd ..

./venv/scripts/activate

python -m src.nodes.cache_node --port $port