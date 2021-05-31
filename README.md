# FW-DDSM

A python package for implementing the Frank-Wolfe-based distributed demand scheduling method. 

`pip install fw-ddsm`

# Setting up the ubuntu envrionment for using FW-DDSM

1. install python3 (should come with ubuntu v20
2. install pip
`sudo apt-get install python3-pip`
3. install dependencies
`pip3 install fw-ddsm pandas pandas_bokeh numpy more-itertools`
4. install snap
`sudo apt-get install snapd`
5. install minizinc bundle
`sudo snap install minizinc --classic`
   
# Features of FW-DDSM

1. Pricing master problem

Minimise the inconvenience, consumption cost and the peak-to-average ratio of the aggregate demand profile of all households. 

2. Household subproblem

Scheduling both the appliances and the batteries to minmise the cost, the inconvenience and the peak-to-average ratio of the household. 

