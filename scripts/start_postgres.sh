docker run --name data_funnel_postgres \
    --rm \
    -e POSTGRES_USER=user \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=database \
    -p 5432:5432 \
    -d postgres
