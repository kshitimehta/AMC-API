# Emissions calculations for AMC
# Authors: Augusto Espin
# DS4CG 2020
# UMass

# This structure defines all parameters used in the calculation
# Factors are the emission factors (CO2/mi)
# Ocupants are the number of ocupants per vehicle type

parameters = { 'factors':   {'car':0.000337609,
                             'light_truck':0.00046428,
                             'bus': 0.0027},
               'ocupants':  {'car':2.5,
                             'bus':20,
                             'group':50},
               'group_type': ['mtnclass','yop-ds','yop-it'],
             }

def ghg_calc(group_size, in_drv_d, out_drv_d, group_type, parameters, ratio, use_bus = 'no'):
    '''
    Computes emissions for combinatios of car and light truck and it can consider a bus
    over the defined number of ocupants when use_bus is True. Returns the calculation for 
    a single case in any possible ratio 
    '''
    factors = parameters['factors']
    ocupants = parameters['ocupants']
    group_types = set(parameters['group_type'])
    # This factor is common for all cases
    factor = (1-ratio)*factors['car'] + ratio*factors['light_truck']
    distance = in_drv_d + out_drv_d
    n_cars = group_size/ocupants['car']
    # Check if calculation will use bus
    # We check the group size and depending on that we compute 
    # the emissions as bus or car
    if use_bus == 'bus':
        if group_size > ocupants['bus']: 
            n_cars = group_size/ocupants['bus']
            factor = factors['bus']
    # Use specific groups to consider bus
    elif use_bus == 'group':
        if group_type in group_types:
            n_cars = group_size/ocupants['group']
            factor = factors['bus'] 

    return n_cars*factor*distance
    