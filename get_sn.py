from datetime import date
import pandas as pd

from odoo_some_company import live

pd.set_option('display.max_columns', 9)

def get_df(so): 
    '''It creates a df with 4 columns.
       sale order name, product name, quantity of the stock move and stock move id.
    '''

    sql = f'''select so.name sale_order
                     ,pp.default_code pn
                     --,sol.name description
                     --,sol.product_uom_qty sol_quantity
                     ,sm.product_uom_qty sm_quantity
                     ,sm.id "sm_id aka sn"
                     --,sol.serial_number sol_sn
                     --,sp.name sp_name
                     --,sol.id sol_id
                     --,sol.status sol_status
                     --,sm.state sm_state
                from sale_order so
                     left join sale_order_line sol on so.id = sol.order_id
                     left join product_product pp on sol.product_id = pp.id
                     left join stock_move sm on sol.id = sm.sale_order_line_id
                     left join stock_picking sp on sm.picking_id = sp.id
               where so.name ~* '{so}'
                     and sol.status !~*'^d$|^td$'
                     and sm.state !~* 'cancel'
                     and sp.name ~* '^rol.(?:wip.out|pick)'
               order by so.id, sol.id
    '''
    return pd.read_sql(sql, live)                 

def hehe(input_file='input_file.csv'):
    '''A generator.
       Maybe it's a better idea to modify "where" clause.
    '''
    for so in pd.read_csv(input_file).sale_order:
        yield get_df(so)

def output(df, output_file='output_filename'):
    d = date.strftime(date.today(), '%y%m%d')
    df.to_csv(f'{output_file}_{d}.csv', index = None)
    print('job done')

def main():

    df = pd.concat(hehe())
    output(df)

if __name__ == '__main__':
    main()
