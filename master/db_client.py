import logging
from ipaddress import IPv4Address
from typing import Any, List, Optional
import json
import asyncpg
from asyncpg import Connection
import traceback
import asyncio
logger = logging.getLogger(__name__)
class DBClient:
    """
    DBhelper. Устанавливает соединение
    Получает записи с БД
    """
    def __init__(self,
                 host: str, port: int, database: str,
                 user: str, password: str, timeout: int = 5):

        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.timeout = timeout
        self.pool: asyncpg.Pool | None = None

    async def create_pool(self):
        try:
            self.pool = await asyncpg.create_pool(
                user=self.user,
                password=self.password,
                database=self.database,
                host=self.host,
                port=self.port,
                max_size=100
            )
            logger.info("[Master] Пул соединений успешно создан")
        except Exception as e:
            logger.error(f"[Master] Ошибка при создании пула соединений: {e}", exc_info=True)
            raise
    async def get_connection(self) -> Connection | None:
        """ Create connection """
        # logger.info(f"[Master] Устанавливаю соединение с БД")
        _try = 5
        while True and _try:
            _try-=1
            try:
                connection = await asyncpg.connect(
                    user=self.user, password=self.password,
                    database=self.database, host=self.host, port=self.port,
                    timeout=self.timeout
                )
                return connection
            except Exception as e:
                error_details = traceback.format_exc()
                logger.error(f"[Master] При подключении к БД произошла ошибка! {e} - {error_details}", exc_info=True)
            await asyncio.sleep(1)
        return None

    async def modify_record(self, bind_id: str):
        async with self.pool.acquire() as conn:
            query = """
                INSERT INTO network_bindstatus_real (id, device_id, parent_id, net_id, ip, mac, port, mode)
            SELECT 
                view.id,
                view.device_id,
                view.parent_id,
                view.net_id,
                view.ip,
                view.mac,
                view.port,
                view.mode
            FROM network_bindstatus_view AS view
            WHERE view.id = $1
            ON CONFLICT (id)
            DO UPDATE SET
                device_id = EXCLUDED.device_id,
                parent_id = EXCLUDED.parent_id,
                net_id = EXCLUDED.net_id,
                ip = EXCLUDED.ip,
                mac = EXCLUDED.mac,
                port = EXCLUDED.port,
                mode = EXCLUDED.mode;
                """
            await conn.execute(query, bind_id)

    async def delete_record(self, id):
        async with self.pool.acquire() as conn:
            # conn = await self.get_connection()
            query = """
                DELETE FROM network_bindstatus_real
                WHERE id = $1
                """
            await conn.execute(query, id)

    async def modify_record_gpon(self, id):
        async with self.pool.acquire() as conn:
            #                     , aserver_id
            #                     view.aserver_id,
            #                     aserver_id = EXCLUDED.aserver_id,
            query = """
                INSERT INTO network_gponbind_cache (id, onu_id, ip, mode, mac)
                SELECT 
                    view.id,
                    view.onu_id,
                    view.ip,
                    view.mode,
                    view.mac
                FROM network_gponbind AS view
                WHERE view.id = $1
                ON CONFLICT (id)
                DO UPDATE SET
                    onu_id = EXCLUDED.onu_id,
                    ip = EXCLUDED.ip,
                    mode = EXCLUDED.mode,
                    mac = EXCLUDED.mac;
                """
            await conn.execute(query, int(id))

    async def delete_record_gpon(self, id):
        async with self.pool.acquire() as conn:
        # conn = await self.get_connection()
            query = """
                DELETE FROM network_gponbind_cache
                WHERE id = $1
                """
            await conn.execute(query, int(id))

    async def fetch_many(self, query: str) -> list[any] | None:
        """ Fetch DB records """
        logger.debug(f"fetch_many:SQL query={query}")
        async with self.pool.acquire() as conn:
            conn: asyncpg.Connection
            return await conn.fetch(query)

    async def get_data_variable(self, name: str) -> str | None:
        """ Get data var from DB """
        where = f'WHERE name=\'{name}\''
        result = await self.fetch_many("""
            SELECT data 
            FROM
                system_datavariable
            {}
            LIMIT 1;
         """.format(where))
        try:
            result = str(result[0]['data']) if result[0]['data'] is not None else None
        except:
            result = None
        return result

    async def get_gpon_bind_list_to_sync(self, active: Optional[list[str]] = None) -> list[dict]:
        where = ''
        if isinstance(active, list) and len(active):
            where = 'WHERE ip NOT IN ({})'.format(
                ','.join([f"\'{str(x).split('-')[0]}\'" for x in active])
            )
            #
            # old network_gponbind_diff
        result = await self.fetch_many(
            """
            SELECT ip, mac, change_type, mode, id 
            FROM view_gpon_bind_diff
            {}
            LIMIT 50;
        """.format(where))

        task = [{"ip": str(row["ip"]), "mac": row["mac"], "change_type":row["change_type"],"mode": row["mode"], "id":row["id"]} for row in result]
        return task

    async def get_device_ids(self,  active: list[str] = None) -> list[int]:
        """
        Получаем PK(id) свитчеq, на которых нужно сделать синхронизацию
        :param active: list активных задач

        :return: list[] PK свитчей
        """

        # Если есть в работе задачи
        if isinstance(active, list) and len(active): # если active это список и он не пустой
            # строка с исключением
            where = 'WHERE device_ip NOT IN ({})'.format(
                ','.join(f"'{el.split('-')[0]}'" for el in active)
            )
        else:
            where = ''

        # Формируем запрос к БД
        query = """
            SELECT DISTINCT
                device_id
            FROM
                network_bind_diff
            {}
            LIMIT 10;
         """.format(where)

        result = await self.fetch_many(query)

        device_ids = [row["device_id"] for row in result]
        return device_ids

        # новая логика
        # _query1 = """
        #     SELECT DISTINCT
        #         device_id
        #     FROM
        #         view_ethernet_bind_diff
        #     {}
        #     LIMIT 10;
        #  """.format(where)

        # result1 = await self.fetch_many(_query1)
        # Объединяем и убираем дубликаты
        # device_ids = {row["device_id"] for row in result + result1}



    async def get_switch_list_to_sync(self, active: Optional[list[str]] = None) -> list[dict]:
        """
        Получаем информацию о свичах и биндах с которыми нужно провести синхронизацию.

        :param active: list уже запущенных задач

        :return: list[dict] список словарей
        """

        # Получаем list pk(id) свитчей
        device_ids = await self.get_device_ids(active)
        device_ids_str = ','.join([str(item) for item in device_ids])

        # device_ids_str = ','.join([str(id['device_id']) for id in device_ids if id['device_id'] is not None])
        if not len(device_ids) or not device_ids_str:
            return []

        logger.info(f"[Master] device_ids формируются для работы: ({device_ids_str})")

        query = """
         WITH device_info AS (
            SELECT nd.id, nd.ip AS device_ip, nd.community, nd.device_model_id
            FROM network_device nd
            WHERE nd.id IN ({})
        ),
        device_model AS (
            SELECT sdm.id, sdm.name AS device_name
            FROM storehouse_devicemodel sdm
            WHERE sdm.id IN (SELECT device_model_id FROM device_info)
        ),
        bind_status AS (
            SELECT 
                nb.id AS bind_id, 
                nb.ip AS bind_ip, 
                nb.port, 
                nb.net_id, 
                nb.mode, 
                nb.device_id,
                nb.change_type
            FROM network_bind_diff nb
            WHERE nb.device_id IN ({})
        ),
        net_info AS (
            SELECT nn.id, CASE 
                WHEN nn.netclass = '1' THEN 'INET'
                WHEN nn.netclass = '2' THEN 'NAT'
                ELSE NULL
            END AS netclass
            FROM network_net nn
            WHERE nn.id IN (SELECT net_id FROM bind_status)
        )
        
        SELECT 
            di.device_ip, 
            di.community, 
            dm.device_name,
            json_agg(json_build_array(bs.bind_ip, bs.port, ni.netclass, bs.mode, bs.change_type, bs.bind_id)) AS bind_info
        FROM device_info di
        LEFT JOIN device_model dm ON di.device_model_id = dm.id
        LEFT JOIN bind_status bs ON di.id = bs.device_id
        LEFT JOIN net_info ni ON bs.net_id = ni.id
        GROUP BY di.device_ip, di.community, dm.device_name;
        """.format(device_ids_str, device_ids_str)

        result = await self.fetch_many(query)
        formatted_result = []

        # device_ip community device_name bind_info
        for row in result:
            task = {
                "device_ip": str(row["device_ip"]),
                "community": row["community"],
                "device_name": row["device_name"],
                "binds": json.loads(row['bind_info']) if row['bind_info'] else []
            }
            formatted_result.append(task)

        return formatted_result
        # query1 = """
        #          WITH device_info AS (
        #             SELECT nd.id, nd.ip AS device_ip, nd.community, nd.device_model_id
        #             FROM network_device nd
        #             WHERE nd.id IN ({})
        #         ),
        #         device_model AS (
        #             SELECT sdm.id, sdm.name AS device_name
        #             FROM storehouse_devicemodel sdm
        #             WHERE sdm.id IN (SELECT device_model_id FROM device_info)
        #         ),
        #         bind_status AS (
        #             SELECT
        #                 nb.id AS bind_id,
        #                 nb.ip AS bind_ip,
        #                 nb.port,
        #                 nb.net_id,
        #                 nb.mode,
        #                 nb.device_id,
        #                 nb.change_type,
        #                 nb.parent_id
        #             FROM view_ethernet_bind_diff nb
        #             WHERE nb.device_id IN ({})
        #         ),
        #         net_info AS (
        #             SELECT nn.id, CASE
        #                 WHEN nn.netclass = '1' THEN 'INET'
        #                 WHEN nn.netclass = '2' THEN 'NAT'
        #                 ELSE NULL
        #             END AS netclass
        #             FROM network_net nn
        #             WHERE nn.id IN (SELECT net_id FROM bind_status)
        #         )
        #
        #         SELECT
        #             di.device_ip,
        #             di.community,
        #             dm.device_name,
        #             json_agg(json_build_array(bs.bind_ip, bs.port, ni.netclass, bs.mode, bs.change_type, bs.bind_id, bs.parent_id)) bind_info
        #         FROM device_info di
        #         LEFT JOIN device_model dm ON di.device_model_id = dm.id
        #         LEFT JOIN bind_status bs ON di.id = bs.device_id
        #         LEFT JOIN net_info ni ON bs.net_id = ni.id
        #         GROUP BY di.device_ip, di.community, dm.device_name;
        #         """.format(device_ids_str, device_ids_str)
        #
        # result1 = await self.fetch_many(query1)

