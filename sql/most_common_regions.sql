select * from (
select region,total,row_number() over (partition by '' order by total desc)rn from (
SELECT region,count(*) total FROM trips_data
group by region
order by 1 desc) a)b 
where rn in (1,2)