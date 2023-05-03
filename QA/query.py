import numpy as np
import pandas as pd
import psycopg2
import asyncio
import functools
import redis

# Creamos un cliente de Redis
redis_client = redis.Redis(host='localhost', port=6379)


# Decorador para cachear resultados de la función en Redis
def cache_function_results(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        # Generamos la clave del caché
        cache_key = f"{function.__name__}:{args}:{kwargs.items()}"

        # Intentamos obtener el resultado del caché
        cached_result = redis_client.get(cache_key)

        # Si existe el resultado en caché, lo devolvemos
        if cached_result is not None:
            return pd.read_json(cached_result)

        # Si no existe el resultado en caché, ejecutamos la función y lo almacenamos en caché
        result = function(*args, **kwargs)
        redis_client.set(cache_key, result.to_json())
        return result

    return wrapper


def query_odoo():
    # Connection postgres
    parameters = {
        "host": "10.150.4.172",
        "port": "5433",
        "user": "lookerstudio",
        "password": "R3porte4d0r.",
        "database": "raloy_productivo"
    }

    conn = psycopg2.connect(**parameters)
    cur = conn.cursor()

    # Start the query
    cur.execute("""select tfpemdf.id_remision,
        tfpemdf.cliente,
        tfpemdf.litros,
        tfpemdf.determinante,
        tfpemdf.pedido_cliente,
        tfpemdf.pedido_raloy,
        tfpemdf.frr,
        tfpemdf.fp,
        case
                        when tfpemdf.dias = '1' then tfpemdf.fpemdf + interval '24 hours'
                        when tfpemdf.dias = '2' then tfpemdf.fpemdf + interval '48 hours'
                        when tfpemdf.dias = '3' then tfpemdf.fpemdf + interval '72 hours'
                        when tfpemdf.dias = '4' then tfpemdf.fpemdf + interval '96 hours'
                        when tfpemdf.dias = '5' then tfpemdf.fpemdf + interval '120 hours'
                        when tfpemdf.dias = '6' then tfpemdf.fpemdf + interval '144 hours'
                        when tfpemdf.dias = '7' then tfpemdf.fpemdf + interval '168 hours'
                        when tfpemdf.dias = '8' then tfpemdf.fpemdf + interval '192 hours'
                        when tfpemdf.dias = '9' then tfpemdf.fpemdf + interval '216 hours'
                        when tfpemdf.dias = '10' then tfpemdf.fpemdf + interval '240 hours'
                        else tfpemdf.fpemdf + interval '24 hours'
        end as fpen0,
        tfpemdf.destino_final,
        tfpemdf.dias, 
        to_char(tfpemdf.fecha_cierre,'yyyy-mm-dd HH24:MI:SS') fecha_cierre,
        tfpemdf.id_carta_porte,
        tfpemdf.carta_porte
        from (	
    select tfpemfs.id_remision,
        tfpemfs.cliente,
        tfpemfs.litros,
        tfpemfs.determinante,
        tfpemfs.pedido_cliente,
        tfpemfs.pedido_raloy,
        tfpemfs.frr,
        tfpemfs.fp,
            case 
            when ( cast(tfpemfs.fpemfs as date) = '2023-02-06'
                    or cast(tfpemfs.fpemfs as date) = '2023-03-20'
                    or cast(tfpemfs.fpemfs as date) = '2023-05-01'
                    or cast(tfpemfs.fpemfs as date) = '2023-11-20'
                    or cast(tfpemfs.fpemfs as date) = '2023-12-25'	
            ) then (tfpemfs.fpemfs) +  interval '24 hours'
            when ( cast(tfpemfs.fpemfs as date) = '2023-04-06'	
            ) then (tfpemfs.fpemfs) +  interval '96 hours'
            else (tfpemfs.fpemfs)
        end as fpemdf,
        tfpemfs.destino_final,
        tfpemfs.dias, 
        tfpemfs.fecha_cierre, 
        tfpemfs.id_carta_porte,
        tfpemfs.carta_porte
    from (	
    select tfpem0.id_remision,
        tfpem0.cliente,
        tfpem0.litros,
        tfpem0.determinante,
        tfpem0.pedido_cliente,
        tfpem0.pedido_raloy,
        tfpem0.frr,
        tfpem0.fp,
    case 
            when (extract( dow from tfpem0.fpem0 ) =  5) then (tfpem0.fpem0) + interval '72 hours'
            when (extract( dow from tfpem0.fpem0 ) =  6) then (tfpem0.fpem0) + interval '48 hours'
            when (extract( dow from tfpem0.fpem0 ) =  0) then (tfpem0.fpem0) + interval '24 hours'
            else tfpem0.fpem0
        end as fpemfs,
        tfpem0.destino_final,
        tfpem0.dias, 
        tfpem0.fecha_cierre, 
        tfpem0.id_carta_porte,
        tfpem0.carta_porte
    from (
    select tfprds.id_remision,
        tfprds.cliente,
        tfprds.litros,
        tfprds.determinante,
        tfprds.pedido_cliente,
        tfprds.pedido_raloy,
        tfprds.frr,
        tfprds.fp,
        (tfprds.fprdf + interval '24 hours') as fpem0,
        tfprds.destino_final,
        tfprds.dias, 
        tfprds.fecha_cierre, 
        tfprds.id_carta_porte,
        tfprds.carta_porte
    from (
    select tfprfs.id_remision,
        tfprfs.cliente,
        tfprfs.litros,
        tfprfs.determinante,
        tfprfs.pedido_cliente,
        tfprfs.pedido_raloy,
        tfprfs.frr,
        tfprfs.fp,
        case 
            when ( cast(tfprfs.fprfs as date) = '2023-02-06'
                    or cast(tfprfs.fprfs as date) = '2023-03-20'
                    or cast(tfprfs.fprfs as date) = '2023-05-01'
                    or cast(tfprfs.fprfs as date) = '2023-11-20'
                    or cast(tfprfs.fprfs as date) = '2023-12-25'	
            ) then (tfprfs.fprfs) +  interval '24 hours'
            when ( cast(tfprfs.fprfs as date) = '2023-04-06'	
            ) then (tfprfs.fprfs) +  interval '96 hours'
            else (tfprfs.fprfs)
        end as fprdf,
        tfprfs.destino_final,
        tfprfs.dias, 
        tfprfs.fecha_cierre, 
        tfprfs.id_carta_porte,
        tfprfs.carta_porte
        from 
    (
    select 
    tfprc.id_remision,
        tfprc.cliente,
        tfprc.litros,
        tfprc.determinante,
        tfprc.pedido_cliente,
        tfprc.pedido_raloy,
        tfprc.frr,
        tfprc.fp,
        case 
            when (extract( dow from tfprc.fprc ) =  5) then (tfprc.fprc) + interval '72 hours'
            when (extract( dow from tfprc.fprc ) =  6) then (tfprc.fprc) + interval '48 hours'
            when (extract( dow from tfprc.fprc ) =  0) then (tfprc.fprc) + interval '24 hours'
            else tfprc.fprc
        end as fprfs,
    tfprc.destino_final,
        tfprc.dias, 
        tfprc.fecha_cierre, 
        tfprc.id_carta_porte,
        tfprc.carta_porte
    from
    (	
    select
        tfpr0.id_remision,
        tfpr0.cliente,
        tfpr0.litros,
        tfpr0.lista_de_precios,
        tfpr0.determinante,
        tfpr0.pedido_cliente,
        tfpr0.pedido_raloy,
        tfpr0.frr,
        tfpr0.fp, 
        case 
            when tfpr0.cliente like any(array[
            '%VALVOLINE INTERNATIONAL DE MEXICO S. DE R.L. DE C.V.%',
    '%PUMA ENERGY SUPPLY & TRADING PTE LTD(PANAMA BRANCH)%',
    '%HELLAMEX, S.A. DE C.V.%',
    '%FRAM GROUP OPERATIONS MEXICO CITY, S.A. DE C.V.%',
    '%LUKOIL LUBRICANTS MEXICO, S DE R.L. DE C.V.%',
    '%LUBRICANTES FUCHS DE MEXICO, S.A. DE C.V.%',
    '%VALVOLINE INTERNATIONAL%',
    '%MAYOREO DE AUTOPARTES Y ACEITES S.A. DE C.V.%',
    '%CENTRO DE DISTRIBUCION ORIENTE, S.A. DE C.V.%',
    '%AGCO MEXICO, S. DE R.L. DE C.V.%',
    '%CUMMINS COMERCIALIZADORA, S. DE R.L. DE C.V.%',
    '%AGENCIA VENDEDORA DE AUTOPARTES, S.A. DE C.V.%',
    '%VAPORMATIC DE MEXICO, S.A. DE C.V.%',
    '%ASG OPERATIONS MEXICO S. DE R.L. DE C.V%',
    '%FUJI LUBRICANTES S.A.S. DE C.V.%',
    '%POWER MIX DE MEXICO S.A. DE C.V. SOFOM ENR%',
    '%PUMA ENERGY SUPPLY AND TRADING PTE LTD (MONTEVIDEO BRANCH)%',
    '%MEXCORP GROUP, INC.%',
    '%PRODUCTOS MCH, S.A. DE C.V.%',
    '%GRUPO NCH, S.A. DE C.V.%',
    '%AGROSERVICIOS DEL NORTE, S.A. DE C.V.%',
    '%PL NA MEXICO S. DE R.L. DE C.V.%',
    '%LUBRICANTES INTELIGENTES DE MEXICO S.A. DE C.V.%',
    '%CAUCHO DEL SURESTE S.R.L. DE C.V.%',
    '%PUMA ENERGY SUPPLY & TRADING PTE. LTD%',
    '%SUBARU AUTOMOTRIZ MEXICO S.A. DE C.V.%',
    '%HELLA AUTOMOTIVE SALES INC.%',
    '%CEMEX TRANSPORTE S.A. DE C.V.%',
    '%LUBRICANTES ECOLOGICOS DEL BAJIO, S.A. DE C.V.%',
    '%CEMEX, S.A.B. DE C.V.%',
    '%CEMEX OPERACIONES MEXICO S.A. DE C.V.%']
            ) then (tfpr0.fpr0) + interval '120 hours'
    --faltan los de a. morales
    when tfpr0.lista_de_precios like any(array[ '%DIST-ESPECIALES-CONRV-RALOY%',
    '%DIST-ESPECIALES-SINRV-KRONEN%',
    '%DIST-ESPECIALES-SINRV-RALOY%']) then (tfpr0.fpr0) + interval '120 hours'
    else (tfpr0.fpr0) + interval '48 hours'
    end as fprc,
        tfpr0.destino_final,
        tfpr0.dias, 
        tfpr0.fecha_cierre, 
        tfpr0.id_carta_porte,
        tfpr0.carta_porte
    from
        (
        select
            case
                when (tfp.hora_fp<12) then (tfp.fp)
                when (tfp.hora_fp >= 12) then (tfp.fp)+ interval '24 hours'
            end as fpr0
    ,
            *
        from
            (
            select
                sp.id as id_remision,
                                    rp.display_name as cliente,
                                    sp.name as remision,
                                    sum(spo.udv_total) as litros,
                                    ppl.name as lista_de_precios,
                                    rp.name as determinante,
                                    sp.client_order_ref as pedido_cliente,
                                    sp.origin as pedido_raloy,
                                    (sp.date_done at time zone 'UTC')as frr,
                                    (so.date_order at time zone 'UTC') as fp,
                                    extract (hour
            from
                (so.date_order at time zone 'UTC')) as hora_fp,
                                    ccp.name as destino_final,
                                    (ccp.dias) as dias,
                                    (cp.fecha_cierre at time zone 'UTC') as fecha_cierre,
                                    cp.id as id_carta_porte,
                                    cp.name as carta_porte
            from
                stock_picking sp
            inner join sale_order so on
                sp.origin = so.name
            left join product_pricelist ppl on
            so.pricelist_id = ppl.id
            left join stock_pack_operation spo on
                sp.id = spo.picking_id
            left join carta_porte_line cpl on
                sp.id = cpl.remision_id
            left join carta_porte cp on
                cpl.carta_id = cp.id
            left join res_partner rp on
                sp.partner_id = rp.id
            left join ciudades_carta_porte ccp on
                cp.ciudad_id = ccp.id
            where
                ((so.date_order at time zone 'UTC')::date >= '2023-01-01')
                and sp.state not like 'cancel'
            group by	
                                    cliente,
                                    id_remision,
                                    remision,
                                    determinante,
                                    pedido_cliente,
                                    pedido_raloy,
                                    lista_de_precios,
                                    fp,
                                    frr,
                                    destino_final,
                                    id_carta_porte,
                                    carta_porte,
                                    dias,
                                    fecha_cierre
                ) tfp
                ) tfpr0
                ) tfprc
                ) tfprfs
                ) tfprds
                ) tfpem0
                ) tfpemfs
                ) tfpemdf""")

    # Get column names from cursor description
    column_names = [desc[0] for desc in cur.description]

    resultados = cur.fetchall()
    cur.close()
    conn.close()

    dates = pd.DataFrame(resultados, columns=column_names).fillna('')

    return dates


# Decorar la función query_products con el decorador de caché
@cache_function_results
def cached_query_odoo():
    return query_odoo()


if __name__ == "__main__":
    cached_query_odoo()

