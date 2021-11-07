with dbt_transactions as (
    select
        turn_id,
        player_id,
        game_id,
        position_id,
        sum(turn_count) as number_of_turns
    from {{ source('dbt_battleship', 'dbt_transaction') }}

    group by 1
)

select
    dbt_transactions.game_id,
    dbt_transactions.turn_id,
    dbt_transactions.player_id,
    dbt_transactions.position_id,
    dbt_transactions.number_of_turns,
    dbt_target_ship_position.ship_id,
    dbt_target_ship_position.target_player_id,
    dbt_target_ship_position.is_occupied_flag,
    dbt_ship.ship_name,
    dbt_ship.ship_hole,
    dbt_player.player_name,
    dbt_game_detail.status,
    case 
            when is_occupied_flag = 'TRUE' 
            then 1 
            else 0 
            end as dbt_hit
    

from dbt_transactions

left join {{ source('dbt_battleship', 'dbt_target_ship_position') }} on dbt_transactions.position_id = dbt_target_ship_position.position_id 
left join {{ source('dbt_battleship', 'dbt_ship') }} using (ship_id) As dbt_main1
left join {{ source('dbt_battleship', 'dbt_player') }} using (player_id) As dbt_main2
left join {{ source('dbt_battleship', 'dbt_game_detail') }} on dbt_transactions.game_id = dbt_game_detail.game_id and dbt_transactions.player_id = dbt_game_detail.player_id



