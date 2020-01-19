from datetime import date
import pandas as pd

from connections import live

pd.set_option('display.max_columns', 9)

def get_sm_id(so):
    '''
    It creates a df with 5 columns.
    sale_order, pn, attribute, quantity and sn
    '''

    sql = f'''
        select so.name "sale_order"
               ,product.default_code pn
               ,pav.name "attribute"
               ,pt.name short_description
               ,coalesce(sml.qty_done, sm.product_uom_qty) quantity
               ,sm.id "sn"
          from sale_order "so"
               join sale_order_line sol on so.id = sol.order_id
               join stock_move sm on sol.id = sm.sale_line_id
               -- pn with attributes
               join product_product product on sm.product_id = product.id
               join product_template pt on product.product_tmpl_id = pt.id
               left join product_attribute_value_product_product_rel rel on product.id = rel.product_product_id
               left join product_attribute_value pav on rel.product_attribute_value_id = pav.id
               join stock_move_line sml on sm.id = sml.move_id -- options for quantity
               join stock_location dest on sm.location_dest_id = dest.id -- to use 'usage' filter
         where sm.state not in ('draft', 'cancel')
               and dest.usage in ('internal', 'customer', 'transit')
               and product.default_code ~* '^[pa]-|^\d'
               and so.name = '{so}'
    '''
    return pd.read_sql(sql, live)

def sm_id_gen(input_file='input_file.csv'):
    '''A generator.
    '''
    for so in pd.read_csv(input_file).sale_order:
        df = get_sm_id(so)
        if df.empty:
            print(f'{so} is empty for sn')
        else:
            yield df

def output(df, output_file='karen'): # karen is a filename
    d = date.strftime(date.today(), '%Y%m%d')
    df.to_csv(f'{output_file}_{d}.csv', index = None)
    print('job done')

def main():

    sn_df = pd.concat(sm_id_gen())
    output(sn_df)

if __name__ == '__main__':
    main()
