

export function ecom_event (event_type: string,p_name: string){
    var data = require('./data.json');

    window.dataLayer.push({ ecommerce: null });  // Clear the previous ecommerce object.
        window.dataLayer.push({
        event: event_type,
        ecommerce: {
            currency: "USD",
            value: 123,
            items:[
                {
                    item_name:p_name,
                    price:123
                }
            ]
        }
        });

    console.log(event_type,p_name);
}