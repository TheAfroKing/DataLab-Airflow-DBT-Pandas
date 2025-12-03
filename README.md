# Classic Models ELT with Airflow and dbt

Este repositorio contiene un pipeline ELT completo que extrae datos de una base de datos MySQL, los carga en un Data Warehouse (MySQL o Vertica) y los transforma utilizando dbt. Todo orquestado por Apache Airflow.
## Arquitectura
![Diagrama de arquitectura](https://github.com/TheAfroKing/Airflow-DBT-DataLab/blob/main/Arquitecture.png)

## Requisitos Previos

- Python 3.8+
- MySQL Server (para la fuente de datos y el DWH si se usa MySQL)
- Vertica (opcional, si se usa como DWH)

## Configuración Rápida

Sigue estos pasos para levantar el entorno localmente:

### 1. Configuración Inicial

Ejecuta el script de instalación para crear el entorno virtual e instalar las dependencias:

```bash
./setup.sh
```

### 2. Levantar Infraestructura (Base de Datos)

Este repositorio incluye un archivo `docker-compose.yml` para levantar una instancia de MySQL (con los datos de prueba) y una instancia de Vertica.

```bash
docker-compose up -d
```

Esto iniciará:
- **MySQL** en el puerto `3306` (Base de datos: `raw`, Usuario: `user`, Password: `password`, Root Password: `root`).
- **Vertica** en el puerto `5433` (Usuario: `dbadmin`, Sin contraseña).

### 3. Iniciar Airflow

Ejecuta el script de inicio. Esto configurará la variable `AIRFLOW_HOME` en el directorio actual e iniciará Airflow en modo standalone:

```bash
./start.sh
```

Una vez iniciado, verás en la terminal las credenciales de administrador (usuario y contraseña) para acceder a la interfaz web de Airflow en `http://localhost:8080`.

## Configuración de Conexiones

Para que los DAGs funcionen, necesitas configurar las conexiones en Airflow y las variables de entorno para dbt.

### Conexiones de Airflow

Accede a la UI de Airflow (Admin -> Connections) y crea las siguientes conexiones:

1.  **MySQL** (`mysql_conn_id`): Conexión a la base de datos fuente.
2.  **MySQLDWH** (`dwh_conn_id`): Conexión al DWH en MySQL (si aplica).
3.  **VerticaDWH** (`vertica_conn_id`): Conexión al DWH en Vertica (si aplica).

### Variables de Entorno para dbt

El perfil de dbt (`dbt/profiles/profiles.yml`) utiliza variables de entorno. Puedes definirlas en el archivo `start.sh` antes de ejecutar airflow o exportarlas en tu terminal:

- `DBT_HOST`: Host de la base de datos (default: localhost)
- `DBT_PORT`: Puerto (default: 5433)
- `DBT_USER`: Usuario (default: dbadmin)
- `DBT_PASSWORD`: Contraseña
- `DBT_DATABASE`: Base de datos (default: docker)
- `DBT_SCHEMA`: Esquema (default: DWH)

## Estructura del Proyecto

- `dags/`: Contiene los DAGs de Airflow.
- `dbt/`: Contiene el proyecto dbt (aunque el `dbt_project.yml` está en la raíz, los perfiles están aquí).
- `models/`: Modelos de dbt.
- `seeds/`: Seeds de dbt.
- `tests/`: Tests de dbt.

## Comandos Útiles

- Activar entorno virtual: `source venv/bin/activate`
- Ejecutar dbt manualmente: `dbt run` (asegúrate de estar en el entorno virtual y tener las variables de entorno configuradas).
