

export function ecom_event (event_type: string,p_name: string){
    var data = require('./data.json');
    console.log(data)
    console.log(data[p_name])
    console.log(data[p_name])
    window.dataLayer.push({ ecommerce: null });  // Clear the previous ecommerce object.
        window.dataLayer.push({
        event: event_type,
        ecommerce: {
            currency: data[p_name]["currency"],
            value: data[p_name]["value"],
            items:[
                {
                    item_name:p_name,
                    price:data[p_name]["value"]
                }
            ]
        }
        });

    console.log(event_type,p_name);
}