with monthly_revenue as (
    select 
        date(date_trunc('month', payment_date)) as payment_month,
        user_id,
        game_name,
        sum(revenue_amount_usd) as total_revenue
     from project.games_payments as gp
     group by 1,2,3
),
revenue_change as (
    select 
        *,
        date(payment_month - interval '1' month) as previous_month,
        date(payment_month + interval '1' month) as next_month,
        lag(total_revenue) over (partition by user_id order by payment_month) as previous_paid_month_revenue,
        lag(payment_month) over (partition by user_id order by payment_month) as previous_paid_month,
        lead(payment_month) over (partition by user_id order by payment_month) as next_paid_month
    from monthly_revenue
),
revenue_metrics as (
    select 
        payment_month,
        user_id,
        game_name,
        total_revenue,
        case
	        when previous_paid_month is null
	        then total_revenue
        end as new_mrr,
        case 
	        when previous_paid_month = previous_month
	        and total_revenue > previous_paid_month_revenue
	        then total_revenue - previous_paid_month_revenue
        end as expansion_revenue,
        case
        	when previous_paid_month = previous_month
	        and total_revenue < previous_paid_month_revenue
	        then total_revenue - previous_paid_month_revenue 
        end as contraction_revenue,
        case 
        	when previous_paid_month != previous_month
        	and previous_paid_month is not null
        	then total_revenue
        end as back_from_churn_revenue,
        case 
        	when next_paid_month is null
        	or next_paid_month != next_month
        	then total_revenue
        end as churned_revenue,
        case
        	when next_paid_month is null
        	or next_paid_month != next_month
        	then next_month
        end as churn_month
    from revenue_change
)
select
    rm.*,
    gpu.has_older_device_model,
    gpu.age,
    gpu.language
from revenue_metrics as rm
left join project.games_paid_users as gpu using(user_id)
    
    
        
        
        
        
        
        
