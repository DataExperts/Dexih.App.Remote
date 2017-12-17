FROM microsoft/aspnetcore-build:2.0 AS build-env
WORKDIR /app

# Copy everything else and build
COPY ./ dexih.application/dexih.remote/
COPY ../dexih.operations dexih.application/dexih.operations/
COPY ../../../dexih.transforms dexih.transforms/
RUN dotnet restore ./dexih.application/dexih.remote
RUN dotnet publish ./dexih.application/dexih.remote/  -c Release -o out

# Build runtime image
FROM microsoft/aspnetcore:2.0
WORKDIR /app
COPY --from=build-env /app/out .
ENTRYPOINT ["dotnet", "dexih.remote.dll"]