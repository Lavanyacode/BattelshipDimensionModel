with dbt_transaction_aggr as (
    select game_id,player_id,ship_id,
       sum(dbt_hit) as total_hits,
       sum(number_of_turns) as total_turns,
       (18-sum(dbt_hit)) as remaining_shots,
       (sum(number_of_turns)-sum(dbt_hit)) as total_missed_shots,
       case 
       when ship_id = '1111' then (5 - (Sum(dbt_hit)))
       when ship_id = '1112' then (5 - (Sum(dbt_hit)))
       when ship_id = '1113' then (3 - (Sum(dbt_hit)))
       when ship_id = '1114' then (3 - (Sum(dbt_hit)))
       when ship_id = '1115' then (2 - (Sum(dbt_hit)))
       end as sunk_status

    from {{ ref('dbt_battleship') }}
    group by game_id, player_id, ship_id
    order by game_id
)
select dbt_transaction_aggr.game_id,
       dbt_transaction_aggr.player_id,
       dbt_transaction_aggr.ship_id,
       sunk_status,
       total_hits,
       total_missed_shots,
       remaining_shots,
       total_turns

       

from dbt_transaction_aggr

        

