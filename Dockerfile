#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["KVDB/KVDB.csproj", "KVDB/"]
COPY ["KVDB.WebAPI/KVDB.WebAPI.csproj", "KVDB.WebAPI/"]
RUN dotnet restore "KVDB.WebAPI/KVDB.WebAPI.csproj"
COPY . .
WORKDIR "/src/KVDB.WebAPI"
RUN dotnet build "KVDB.WebAPI.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "KVDB.WebAPI.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
VOLUME /data
ENV DataDir=/data
ENTRYPOINT ["dotnet", "KVDB.WebAPI.dll"]