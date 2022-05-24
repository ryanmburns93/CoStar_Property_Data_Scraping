# coding: utf-8

# download Python
# download Chrome Browser
# library import/install is designed for completely fresh VDI
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    from selenium import webdriver
except ModuleNotFoundError:
    install('selenium')
    from selenium import webdriver
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ModuleNotFoundError:
    install('webdriver-manager')
    from webdriver_manager.chrome import ChromeDriverManager

import os
import time
try:
    import pandas as pd
except ModuleNotFoundError:
    install('pandas')
    import pandas as pd
import numpy as np
from glob import glob

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions

import datetime
try:
    from sqlalchemy import create_engine, MetaData, update
    from sqlalchemy.types import BIGINT, INTEGER, VARCHAR, FLOAT, DATETIME
except ModuleNotFoundError:
    install('sqlalchemy')
    from sqlalchemy import create_engine, MetaData, update
    from sqlalchemy.types import BIGINT, INTEGER, VARCHAR, FLOAT, DATETIME
try:
    import pyodbc
except ModuleNotFoundError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyodbc-4.0.32-cp310-cp310-win_amd64.whl'])
    import pyodbc  #Needs to be imported to support string connector of sql alchemy engine
import urllib
try:
    from tqdm import tqdm
except ModuleNotFoundError:
    install('tqdm')
    from tqdm import tqdm

import json
from concurrent.futures import as_completed, ProcessPoolExecutor
try:
    from requests_futures.sessions import FuturesSession
except ModuleNotFoundError:
    install('requests_futures')
    from requests_futures.sessions import FuturesSession
from requests import Session


def launch_webdriver():
    """
    Instantiate the webdriver application with explicitly defined options.

    Options set webdriver to maximized window at start,

    Parameters
    ----------
    webdriver_path : string, optional
        Alternate filepath to directory containing chromedriver.exe.
        The default is None.

    Returns
    -------
    driver : webdriver
        Initialized webdriver for retrieving and manipulating web pages.
    """
    options = ChromeOptions()
    options.add_argument("--log-level=3")
    options.add_argument("start-maximized")
    options.add_experimental_option('excludeSwitches', ['load-extension', 'enable-automation', 'enable-logging'])
    #options.add_argument('--user-data-dir=//aimco.com/data/Departments/Property Ops/Decision Support/CoStarPropertyExport/chromedriver_profile/')
    #options.headless = True
    s=ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s, options=options)
    return driver


def login_to_costar(driver, username_string, password_string):
    """
    Log in to the CoStar website with the provided webdriver.

    Parameters
    ----------
    driver : webdriver
        Active driver (should already be instantiated).
    username_string : str, optional
        The CoStar account username to log into.
    password_string : str, optional
        The CoStar account password to log into the provided username.

    Returns
    -------
    None.

    """
    driver.get('https://gateway.costar.com/login')
    username = driver.find_element(By.ID, "username")
    password = driver.find_element(By.ID, "password")
    login_button = driver.find_element(By.XPATH,
                                       '/html/body/div/div/div/div[1]/div[1]/form/div/div/div[2]/div[4]/div[2]/div/button')
    # need to clear out any saved text or prefilled passwords
    username.clear()
    password.clear()

    actions = ActionChains(driver)
    actions.send_keys_to_element(username, username_string)
    actions.send_keys_to_element(password, password_string)
    actions.click(login_button)
    actions.perform()
    try:
        WebDriverWait(driver,
                      20).until(EC.
                                element_to_be_clickable((By.XPATH,
                                                         '//*[@id="cs-gateway-home-page"]/div[2]/div[1]/div/div/div[2]/div/div[1]/input')))
    except TimeoutException:
        time.sleep(20)
    time.sleep(1)
    return


def load_properties(filepath=None):
    """
    Load the list of properties from the .csv file.

    Parameters
    ----------
    filepath : str, optional
        Location of the .csv file containing the list of comp properties and the
        respective CoStar ID. The default is None, in which case the user will be
        prompted for the file location.

    Returns
    -------
    properties_df : Pandas DataFrame
        A dataframe with the property details in the .csv file.

    """
    if filepath is None:
        filepath = input('Enter the full path to the .csv file containing each property names and ID:')
    properties_df = pd.read_csv(filepath)
    properties_df.dropna(subset=['CoStarPropID'], inplace=True)
    properties_df.reset_index(drop=True, inplace=True)
    properties_df['CoStarPropID'] = [str(int(propid)) for propid in properties_df['CoStarPropID'] if not np.isnan(propid)]
    return properties_df


def get_costar_cookies(username_string, password_string):
    """
    Launch webdriver, login to costar account, and get a dictionary of cookies
    assigned to the webdriver.

    Returns
    -------
    cookies_dict : dict
        Dictionary of the name: value of each cookie assigned to the webdriver post-login.

    """
    driver = launch_webdriver()
    login_to_costar(driver, username_string, password_string)
    time.sleep(5)
    cookies_list = driver.get_cookies()

    cookies_dict = {}
    for cookie in cookies_list:
        cookies_dict[cookie['name']] = cookie['value']
    return cookies_dict


def get_payload(propId):
    """
    Generate and encode the payload for an XHR request for a single property from CoStar.

    Parameters
    ----------
    propId : int
        The unique CoStar Property ID for a single property.

    Returns
    -------
    payload : str
        The JSON-encoded string of the payload for an XHR request for a single property.

    """
    propId = int(propId)

    # Amenities
    payload_amenities = {"operationName": "Amenities_Info",
                         "query": "query Amenities_Info($propertyId: Int!) {\n  propertyDetail {\n    amenities_Info(propertyId: $propertyId) {\n      ...amenitiesICFields\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment amenitiesICFields on Amenities_Info {\n  propertyType\n  amenities\n  unitAmenities\n  roomAmenities\n  __typename\n}\n",
                         "variables": {"propertyId": propId}}

    # UnitMix
    payload_unitmix = {"operationName": "UnitMix_Detail",
                       "variables": {"propertyId": propId,
                                     "showOnlyActual": False},
                       "query": "fragment unitMixDetail on UnitMixDetailItem {\n  totals\n  bath\n  availableUnits\n  availablePercent\n  askingRentPerArea\n  askingRentPerUnit\n  averageArea\n  effectiveRentPerUnit\n  effectiveRentPerArea\n  bedroom\n  concessions\n  unitMixUnits\n  unitMixPercentage\n  isRentModeled\n  isEffectiveRentModeled\n  isAvailableModeled\n  unitMixBeds\n  unitMixBedsPercent\n  averageAskingRentPerOccupantBed\n  averageEffectiveRentPerOccupantBed\n  __typename\n}\n\nquery UnitMix_Detail($propertyId: Int!, $showOnlyActual: Boolean) {\n  propertyDetail {\n    unit_mix_detail(propertyId: $propertyId, showOnlyActual: $showOnlyActual) {\n      detailItems {\n        ...unitMixDetail\n        __typename\n      }\n      summaryItems {\n        ...unitMixDetail\n        __typename\n      }\n      updatedDate\n      __typename\n    }\n    __typename\n  }\n}\n"}

    # Owners
    payload_about_owners = {"operationName": "About_Info",
                            "query": "query About_Info($propertyId: Int!) {\n  propertyDetail {\n    propertyContactDetails_info(propertyId: $propertyId) {\n      ...aboutICFields\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment aboutICFields on PropertyContactDetails_Info {\n  trueOwner {\n    ...companyDetailFields\n    __typename\n  }\n  architect {\n    ...companyDetailFields\n    __typename\n  }\n  __typename\n}\n\nfragment companyDetailFields on PropertyContactDetails_Company {\n  companyId\n  name\n  address\n  suite\n  website {\n    uri\n    hostName\n    __typename\n  }\n  phoneNumbers\n  logoImage\n  availableInProfessionalDirectory\n  dateAssumedRole\n  bio\n  __typename\n}\n",
                            "variables": {"propertyId": propId}}

    # Location
    payload_about_location = {"operationName": "Location_Info",
                              "query": "query Location_Info($propertyId: Int!) {\n  propertyDetail {\n    location_Info(propertyId: $propertyId) {\n      ...locationICFields\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment locationICFields on Location_Info {\n  secondaryDeliveryAddress\n  postalCode\n  submarket\n  cluster\n  locationType\n  market\n  county\n  subDivision\n  regionName\n  cbsa\n  dma\n  country\n  __typename\n}\n",
                              "variables": {"propertyId": propId}}

    # Comps
    payload_comps = {"operationName": "getCompsContext",
                     "query": "query getCompsContext($propertyId: Int!) {\n  propertyDetail {\n    get_comps_context(propertyId: $propertyId) {\n      compsContext {\n        Source\n        Survey {\n          DisplayName\n          SurveyId\n          SurveyName\n          __typename\n        }\n        CompanyCompSetCount\n        MapShape {\n          Radii {\n            Center {\n              coordinates\n              type\n              __typename\n            }\n            City\n            Kml\n            LookupText\n            SubdivisionCode\n            Radius {\n              Code\n              Value\n              __typename\n            }\n            SubdivisionCode\n            __typename\n          }\n          Corridors {\n            Corridor {\n              coordinates\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        CompEntities {\n          EntityId\n          PropertyIndex\n          __typename\n        }\n        __typename\n      }\n      isCollaborationSourceViable\n      detailPageContext {\n        CountryCode\n        Location {\n          Latitude\n          Longitude\n          __typename\n        }\n        PropertyId\n        PropertyTypeDescription\n        PropertyTypeId\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n",
                     "variables": {"propertyId": propId}}

    # Contacts
    payload_contact_details = {"operationName": "ContactsDetail",
                               "query": "query ContactsDetail($propertyId: Int!) {\n  propertyDetail {\n    propertyContactDetails_info(propertyId: $propertyId) {\n      ...contactDetailsICFields\n      __typename\n    }\n    propertyDetailHeader(propertyId: $propertyId) {\n      id\n      addressHeader\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment contactDetailsICFields on PropertyContactDetails_Info {\n  coStarResearchContact {\n    ...propertyCompanyFields\n    __typename\n  }\n  primaryLeasingCompany {\n    ...propertyCompanyFields\n    __typename\n  }\n  leasingCompany {\n    ...propertyCompanyFields\n    __typename\n  }\n  subletCompany {\n    ...propertyCompanyFields\n    __typename\n  }\n  salesCompany {\n    ...propertyCompanyFields\n    __typename\n  }\n  recordedOwner {\n    ...propertyCompanyFields\n    __typename\n  }\n  trueOwner {\n    ...propertyCompanyFields\n    __typename\n  }\n  hotelOperator {\n    ...propertyCompanyFields\n    __typename\n  }\n  assetManager {\n    ...propertyCompanyFields\n    __typename\n  }\n  propertyManager {\n    ...propertyCompanyFields\n    __typename\n  }\n  previousRecordedOwner {\n    ...propertyCompanyFields\n    __typename\n  }\n  previousTrueOwner {\n    ...propertyCompanyFields\n    __typename\n  }\n  parentCompany {\n    ...propertyCompanyFields\n    __typename\n  }\n  developer {\n    ...propertyCompanyFields\n    __typename\n  }\n  architect {\n    ...propertyCompanyFields\n    __typename\n  }\n  coWorkingCompany {\n    ...propertyCompanyFields\n    __typename\n  }\n  deliveryAddress\n  emailSubject\n  __typename\n}\n\nfragment propertyContactFields on PropertyContactDetails_Contact {\n  personId\n  title\n  name\n  email\n  phoneNumbers\n  thumbnail\n  availableInProfessionalDirectory\n  withCompanyId\n  withCompanyName\n  withCompanyAddress\n  managerEmailAddress\n  __typename\n}\n\nfragment propertyCompanyFields on PropertyContactDetails_Company {\n  ...companyDetailFields\n  contacts {\n    ...propertyContactFields\n    __typename\n  }\n  __typename\n}\n\nfragment companyDetailFields on PropertyContactDetails_Company {\n  companyId\n  name\n  address\n  suite\n  website {\n    uri\n    hostName\n    __typename\n  }\n  phoneNumbers\n  logoImage\n  availableInProfessionalDirectory\n  dateAssumedRole\n  bio\n  __typename\n}\n",
                               "variables": {"propertyId": propId}}

    # Property details
    payload_property_details = {"operationName": "getPropertyInfo",
                                "query": "query getPropertyInfo($propertyId: Int!, $currencyCode: String) {\n  propertyDetail {\n    property_info(propertyId: $propertyId, currencyCode: $currencyCode) {\n      ad\n      address {\n        buildingName\n        city\n        cityId\n        county\n        deliveryAddress\n        postalCode\n        countryCode\n        regionId\n        regionName\n        state\n        stateId\n        streetDirId\n        streetName\n        streetNum\n        streetNumEnd\n        streetSuffixId\n        streetTypeId\n        __typename\n      }\n      avgAskingRentPerBed\n      bathBedRatio\n      bldgClass\n      buildingAreaTotal\n      buildingAreaUom\n      buildingName\n      buildingNumber\n      buildingRating\n      buildMonth\n      cluster\n      companies {\n        recordedOwners {\n          companyId\n          name\n          originType\n          ownerSuperTypeId\n          ownerTypeId\n          __typename\n        }\n        trueOwners {\n          companyId\n          name\n          originType\n          ownerSuperTypeId\n          ownerTypeId\n          __typename\n        }\n        __typename\n      }\n      constructionStartMonth\n      constructionStartYear\n      constructionStatus\n      currency\n      floorArea\n      forLease {\n        areaTotal\n        areaUom\n        contigArea\n        rentActMax\n        rentActMin\n        rentBasis\n        rentEstMax\n        rentEstMin\n        spaceCount\n        tmi\n        __typename\n      }\n      forSale {\n        buildingAreaUom\n        capRateMax\n        capRateMin\n        company\n        companyId\n        condoOnly\n        contactFirstName\n        contactId\n        contactLastName\n        contactPhone\n        dateOnMarket\n        itemType\n        phoneCountryCode\n        pricePerAcreMax\n        pricePerBuildingAreaMax\n        pricePerBuildingAreaMin\n        pricePerItemMax\n        pricePerItemMin\n        salePrice\n        statusId\n        __typename\n      }\n      industrial {\n        driveInsHeight\n        driveInsWidth\n        hasCranes\n        hasDriveInBay\n        hasHeavyPower\n        hasLoadingDocks\n        hasRail\n        maxColumnDepthFeet\n        maxColumnWidthFeet\n        numOfCranes\n        numOfDriveIns\n        numOfExternalLoadingDocks\n        numOfInternalLoadingDocks\n        numOfLoadingDocks\n        railLine\n        sewer\n        __typename\n      }\n      institutionName\n      isBreeam\n      isLeed\n      isOpportunityZone\n      landAreaTotal\n      landAreaUom\n      landType\n      latitude\n      longitude\n      mapBookName\n      mapPageNum\n      mapXCoordinate\n      mapYCoordinate\n      market\n      maxCeilingHt\n      metro\n      minAreaMeasure\n      minAreaPerLot\n      minCeilingHt\n      multiFamily {\n        affordabilityType\n        apartmentLifestyleType\n        apartmentRentType\n        apartmentStyleType\n        askingRentPerSqFt\n        askingRentPerUnit\n        avgUnitSize\n        concession\n        effRentPerSqFt\n        effRentPerUnit\n        numOfBuildings\n        numOfUnits\n        parkingSpacesPerUnit\n        percent1Bed\n        percent2Bed\n        percent3Bed\n        percent4Bed\n        percentStudio\n        vacancyPercent\n        __typename\n      }\n      numOfBeds\n      numOfParkingSpaces\n      numOfRooms\n      numOfStories\n      parkingRatio\n      percentLeased\n      primaryLeasingInfo {\n        company\n        companyId\n        contactFirstName\n        contactLastName\n        contactPhone\n        phoneCountryCode\n        __typename\n      }\n      primaryPhoto {\n        caption\n        url\n        __typename\n      }\n      propertyGroupName\n      propertySubtype\n      propertyGroupType\n      propertyId\n      propertySubtype\n      propertyType\n      proposedLandUse\n      renovationMonth\n      renovationYear\n      retailLoc\n      singleTenancyState\n      submarket\n      yearBuilt\n      zoning\n      __typename\n    }\n    __typename\n  }\n}\n",
                                "variables": {"propertyId": propId, "currencyCode": "USD"}}

    payload = [payload_amenities,
               payload_unitmix,
               payload_about_owners,
               payload_about_location,
               payload_comps,
               payload_contact_details,
               payload_property_details]

    payload = json.dumps(payload)
    return payload


def post_to_db(sql_connection_string, sql_table_name, last_scrape_df=None):
    """
    Upload results from last CoStar web scrape into the SQL Server table specified
    by the SQL connection string.

    Parameters
    ----------
    last_scrape_df : dataframe, optional
        The dataframe from the last run scrape. The default is None. If a dataframe
        is not passed in, the method searches for the last saved .csv file in
        the shared drive.
    sql_connection_string : str
        The connection string used by SQL Alchemy to connect to the target table in the
        SQL Server database into which the data is injected.
    sql_table_name : str
        The name of the table into which the data is injected.

    Returns
    -------
    None.

    """
    if last_scrape_df is None:
        files = glob("C:/Users/RBurns/Documents/*.csv")
        files.sort(key=os.path.getmtime)
        data_df = pd.read_csv(files[-1])
    else:
        data_df = last_scrape_df

    quoted = urllib.parse.quote_plus(sql_connection_string)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

    md = MetaData(bind=engine, schema='dbo')
    md.reflect(only=[sql_table_name])
    CoStarPropertyExport_table = md.tables['dbo.' + sql_table_name]
    statement = (update(CoStarPropertyExport_table).
                 where(CoStarPropertyExport_table.c.MostRecentFlag==1).
                 values(MostRecentFlag=0))
    engine.execute(statement)

    data_df = convert_df_types(data_df)
    amenities_shrink_dict = {'24 Hour Access': '24Hr Access',
                             'Air Conditioning': 'A/C',
                             'Bicycle Storage': 'Bike Storage',
                             'Refridgerator': 'Fridge',
                             'Basketball Court': 'Bball Court',
                             'Storage Space': 'Storage',
                             'Walking/Biking Trails': 'Walk/Bike Trails',
                             'Property Manager on Site': 'Onsite PM',
                             'Wheelchair Accessible (Rooms)': 'Wheelchair Access Rooms',
                             'Accessible': 'Access',
                             'Planned Social Activities': 'Social Events',
                             'Maintenance on site': 'Onsite Maintenance',
                             'Furnished Units Available': 'Furnished Option',
                             'Hardwood Floors': 'Hardwood',
                             'Pet Washing Station': 'Pet Wash Station',
                             'Laundry Facilities': 'Laundry Facs',
                             'Tenant Controlled HVAC': 'Controllable HVAC',
                             'Washer/Dryer': 'W/D'}

    # Replace long text elements in amenities_shrink_dict with abbreviated text versions
    for index, amenity_list in enumerate(data_df['Amenities']):
        temp_list_record = amenity_list
        if pd.isnull(temp_list_record):
            temp_list_record = np.nan
        else:
            for key, val in amenities_shrink_dict.items():
                temp_list_record = temp_list_record.replace(key, val)
        data_df.at[index, 'Amenities'] = temp_list_record

    # shorten amenities list string to less than 250 characters (max set by db?)
    for index, amenities in enumerate(data_df['Amenities']):
        if not pd.isnull(amenities):
            if len(amenities) > 250:
                temp_index = amenities[:250].rfind(";")
            else:
                temp_index = len(amenities)
            data_df.at[index, 'Amenities'] = amenities[:temp_index]

    data_df = data_df.assign(CollectedDateStamp=datetime.datetime.today())
    data_df = data_df.assign(MostRecentFlag=1)

    print('Beginning upload to SQL Server database.')
    start = datetime.datetime.now()
    data_df.to_sql(name=sql_table_name,
                   schema="dbo",
                   con=engine,
                   if_exists='append',
                   index=False,
                   chunksize=100,
                   dtype={'CoStarPropertyID': BIGINT,
                          'PropertyName': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'PropertyAddress': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'OneBedroomAskingRentUnit': INTEGER,
                          'TwoBedroomAskingRentUnit': INTEGER,
                          'ThreeBedroomAskingRentUnit': INTEGER,
                          'FourBedroomAskingRentUnit': INTEGER,
                          'StudioAskingRentUnit': INTEGER,
                          'OneBedroomAvgSF': INTEGER,
                          'TwoBedroomAvgSF': INTEGER,
                          'ThreeBedroomAvgSF': INTEGER,
                          'FourBedroomAvgSF': INTEGER,
                          'StudioAvgSF': INTEGER,
                          'OneBedroomEffectiveRentUnit': INTEGER,
                          'TwoBedroomEffectiveRentUnit': INTEGER,
                          'ThreeBedroomEffectiveRentUnit': INTEGER,
                          'FourBedroomEffectiveRentUnit': INTEGER,
                          'StudioEffectiveRentUnit': FLOAT,
                          'NumberOf1BedroomsUnits': INTEGER,
                          'NumberOf2BedroomsUnits': INTEGER,
                          'NumberOf3BedroomsUnits': INTEGER,
                          'NumberOf4BedroomsUnits': INTEGER,
                          'NumberOfStudioUnits': INTEGER,
                          'NumberOfUnits': INTEGER,
                          'OneBedroomConcessionsPercentage': FLOAT,
                          'TwoBedroomConcessionsPercentage': FLOAT,
                          'ThreeBedroomConcessionsPercentage': FLOAT,
                          'FourBedroomConcessionsPercentage': FLOAT,
                          'StudioConcessionsPercentage': FLOAT,
                          'Latitude': FLOAT,
                          'Longitude': FLOAT,
                          'OperationalStatus': FLOAT,
                          'PropertyManagerName': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'OwnerName': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'ParentCompany': FLOAT,
                          'BuildingClass': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'StarRating': BIGINT,
                          'Amenities': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'YearBuilt': FLOAT,
                          'YearRenovated': FLOAT,
                          'PercentLeased': FLOAT,
                          'City': VARCHAR(100, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'State': VARCHAR(2, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'Zip': VARCHAR(10, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'PropertyType': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'BuildingStatus': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'ConstructionStatus': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'TrueOwnerName': VARCHAR(250, collation="SQL_Latin1_General_CP1_CI_AS"),
                          'ParkingSpaces': BIGINT,
                          'BuildingStories': BIGINT,
                          'CollectedDateStamp': DATETIME,
                          'MostRecentFlag': INTEGER})

    end = datetime.datetime.now()
    print(f'Transfer to SQL {sql_table_name} table completed in {end-start}.')
    return


def convert_df_types(test_df=None):
    """
    Convert the type of the dataframe to match the SQL database before injection.

    Parameters
    ----------
    test_df : Pandas DataFrame
        DataFrame containing the data from the most recent CoStar web scrape.

    Returns
    -------
    df : Pandas DataFrame
        DataFrame containing the data from the most recent CoStar web scrape standardized
        to match the datatypes of the CoStarPropertyExport SQL table.

    """
    df = test_df.copy()
    # convert currency to integers
    for col in ['OneBedroomAskingRentUnit',
                'TwoBedroomAskingRentUnit',
                'ThreeBedroomAskingRentUnit',
                'FourBedroomAskingRentUnit',
                'StudioAskingRentUnit',
                'OneBedroomEffectiveRentUnit',
                'TwoBedroomEffectiveRentUnit',
                'ThreeBedroomEffectiveRentUnit',
                'FourBedroomEffectiveRentUnit',
                'StudioEffectiveRentUnit']:
        for index, askingrentunit in enumerate(df[col]):
            if pd.isnull(askingrentunit):
                temp_val = np.nan
            elif len(askingrentunit) > 3:
                temp_val = askingrentunit[1:]
                temp_val = temp_val.replace(',', '')
                temp_val = int(temp_val)
            df.at[index, col] = temp_val

    # convert percentages to floats
    for col in ['OneBedroomConcessionsPercentage',
                'TwoBedroomConcessionsPercentage',
                'ThreeBedroomConcessionsPercentage',
                'FourBedroomConcessionsPercentage',
                'StudioConcessionsPercentage']:
        for index, concessionspercent in enumerate(df[col]):
            if pd.isnull(concessionspercent):
                temp_val = np.nan
            elif len(concessionspercent) > 1:
                temp_val = concessionspercent[:-1]
                temp_val = float(temp_val)
            df.at[index, col] = temp_val

    # convert counts to integers
    for col in ['OneBedroomAvgSF',
                'TwoBedroomAvgSF',
                'ThreeBedroomAvgSF',
                'FourBedroomAvgSF',
                'StudioAvgSF',
                'NumberOf1BedroomsUnits',
                'NumberOf2BedroomsUnits',
                'NumberOf3BedroomsUnits',
                'NumberOf4BedroomsUnits',
                'NumberOfStudioUnits',
                'NumberOfUnits']:
        for index, count_val in enumerate(df[col]):
            if pd.isnull(count_val):
                temp_val = np.nan
            elif len(count_val) > 0:
                if count_val == '-':
                    temp_val = np.nan
                else:
                    temp_val = count_val.replace(',', '')
                    temp_val = int(temp_val)
            df.at[index, col] = temp_val

    return df


def print_url(r, *args, **kwargs):
    """
    Print url of API call response as an assigned hook tagged to each call response.

    Parameters
    ----------
    r : API response.
        The response from the CoStar API call.

    Returns
    -------
    None.

    """
    print(f'Request URL: {r.url}')


def print_status(r, *args, **kwargs):
    """
    Print response status of API call as an assigned hook tagged to each call response.

    Parameters
    ----------
    r : API response.
        The response from the CoStar API call.

    Returns
    -------
    None.

    """
    print(f'Response Status Code: {r.status_code}')


def reissue_call_and_read_response_into_df(df, file, cookies_dict):
    """
    Create single XHR call, send, and save response overwriting last
    response. Called when prior response errored/is empty.

    Parameters
    ----------
    df : Pandas DataFrame
        DataFrame containing target data element values for all property API responses already
        processed from the current scrape. The single property whose call is recorded in the
        .txt file specified by the file variable will be appended to this dataframe.
    file : str
        Full file path to .txt file containing CoStar API response for a single property.
    cookies_dict : dict
        Dictionary of the name: value of each cookie assigned to the webdriver post-login.

    Returns
    -------
    df : Pandas DataFrame
        DataFrame containing the data for properties already parsed from .txt call response files
        onto which the values in file have been concatenated.

    """
    s = Session()
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Accept-Encoding": "*",
        "Connection": "keep-alive",
        'origin': 'https://product.costar.com',
        'x-requested-with': 'XMLHttpRequest'
    }
    s.headers.update(headers)
    s.hooks['response'].append(print_url)
    s.hooks['response'].append(print_status)
    url = 'https://product.costar.com/graphql'
    prop_id = file.split('\\')[1].split('_')[0]
    payload = get_payload(prop_id)
    resp = s.post(url, data=payload, headers=headers, cookies=cookies_dict)
    with open(file, 'w+') as f:
        f.write(resp.text)
        f.close()
    df = read_call_response_into_df(df, file)
    return df


def set_roomtype_metric_values(unit_type, present_in_columns, prop_details_df):
    """
    Pack and return tuple containing metrics for a single type of apartment by number of bedrooms.

    Parameters
    ----------
    unit_type : str
        String description of the apartment type by number of bedrooms.
    present_in_columns : bool
        Boolean indicator whether any metrics for the apartment bedroom type are present in prop_details_df.
    prop_details_df : Pandas DataFrame
        DataFrame representation of the property metrics table by apartment bedroom type and metric.

    Returns
    -------
    unit_type_metrics_package : tuple
        A tuple containing five average metric values measuring the specified apartment bedroom type.

    """
    if unit_type == 'Studio':
        if present_in_columns:
            StudioAskingRentUnit_val = screen_nulls(prop_details_df.at['askingRentPerUnit', 'All Studios'])
            StudioAvgSF_val = screen_nulls(prop_details_df.at['averageArea', 'All Studios'])
            StudioEffectiveRentUnit_val = screen_nulls(prop_details_df.at['effectiveRentPerUnit', 'All Studios'])
            NumberOfStudioUnits_val = screen_nulls(prop_details_df.at['unitMixBeds', 'All Studios'])
            StudioConcessionsPercentage_val = screen_nulls(prop_details_df.at['concessions', 'All Studios'])
        else:
            StudioAskingRentUnit_val = np.nan
            StudioAvgSF_val = np.nan
            StudioEffectiveRentUnit_val = np.nan
            NumberOfStudioUnits_val = np.nan
            StudioConcessionsPercentage_val = np.nan
        unit_type_metrics_package = (StudioAskingRentUnit_val,
                                     StudioAvgSF_val,
                                     StudioEffectiveRentUnit_val,
                                     NumberOfStudioUnits_val,
                                     StudioConcessionsPercentage_val)
    elif unit_type == 'One Bed':
        if present_in_columns:
            OneBedroomAskingRentUnit_val = screen_nulls(prop_details_df.at['askingRentPerUnit', 'All 1 Beds'])
            OneBedroomAvgSF_val = screen_nulls(prop_details_df.at['averageArea', 'All 1 Beds'])
            OneBedroomEffectiveRentUnit_val = screen_nulls(prop_details_df.at['effectiveRentPerUnit', 'All 1 Beds'])
            NumberOf1BedroomUnits_val = screen_nulls(prop_details_df.at['unitMixBeds', 'All 1 Beds'])
            OneBedroomConcessionsPercentage_val = screen_nulls(prop_details_df.at['concessions', 'All 1 Beds'])
        else:
            OneBedroomAskingRentUnit_val = np.nan
            OneBedroomAvgSF_val = np.nan
            OneBedroomEffectiveRentUnit_val = np.nan
            NumberOf1BedroomUnits_val = np.nan
            OneBedroomConcessionsPercentage_val = np.nan
        unit_type_metrics_package = (OneBedroomAskingRentUnit_val,
                                     OneBedroomAvgSF_val,
                                     OneBedroomEffectiveRentUnit_val,
                                     NumberOf1BedroomUnits_val,
                                     OneBedroomConcessionsPercentage_val)
    elif unit_type == 'Two Bed':
        if present_in_columns:
            TwoBedroomAskingRentUnit_val = screen_nulls(prop_details_df.at['askingRentPerUnit', 'All 2 Beds'])
            TwoBedroomAvgSF_val = screen_nulls(prop_details_df.at['averageArea', 'All 2 Beds'])
            TwoBedroomEffectiveRentUnit_val = screen_nulls(prop_details_df.at['effectiveRentPerUnit', 'All 2 Beds'])
            NumberOf2BedroomUnits_val = screen_nulls(prop_details_df.at['unitMixBeds', 'All 2 Beds'])
            TwoBedroomConcessionsPercentage_val = screen_nulls(prop_details_df.at['concessions', 'All 2 Beds'])
        else:
            TwoBedroomAskingRentUnit_val = np.nan
            TwoBedroomAvgSF_val = np.nan
            TwoBedroomEffectiveRentUnit_val = np.nan
            NumberOf2BedroomUnits_val = np.nan
            TwoBedroomConcessionsPercentage_val = np.nan
        unit_type_metrics_package = (TwoBedroomAskingRentUnit_val,
                                     TwoBedroomAvgSF_val,
                                     TwoBedroomEffectiveRentUnit_val,
                                     NumberOf2BedroomUnits_val,
                                     TwoBedroomConcessionsPercentage_val)
    elif unit_type == 'Three Bed':
        if present_in_columns:
            ThreeBedroomAskingRentUnit_val = screen_nulls(prop_details_df.at['askingRentPerUnit', 'All 3 Beds'])
            ThreeBedroomAvgSF_val = screen_nulls(prop_details_df.at['averageArea', 'All 3 Beds'])
            ThreeBedroomEffectiveRentUnit_val = screen_nulls(prop_details_df.at['effectiveRentPerUnit', 'All 3 Beds'])
            NumberOf3BedroomUnits_val = screen_nulls(prop_details_df.at['unitMixBeds', 'All 3 Beds'])
            ThreeBedroomConcessionsPercentage_val = screen_nulls(prop_details_df.at['concessions', 'All 3 Beds'])
        else:
            ThreeBedroomAskingRentUnit_val = np.nan
            ThreeBedroomAvgSF_val = np.nan
            ThreeBedroomEffectiveRentUnit_val = np.nan
            NumberOf3BedroomUnits_val = np.nan
            ThreeBedroomConcessionsPercentage_val = np.nan
        unit_type_metrics_package = (ThreeBedroomAskingRentUnit_val,
                                     ThreeBedroomAvgSF_val,
                                     ThreeBedroomEffectiveRentUnit_val,
                                     NumberOf3BedroomUnits_val,
                                     ThreeBedroomConcessionsPercentage_val)
    elif unit_type == 'Four Bed':
        if present_in_columns:
            FourBedroomAskingRentUnit_val = screen_nulls(prop_details_df.at['askingRentPerUnit', 'All 4 Beds'])
            FourBedroomAvgSF_val = screen_nulls(prop_details_df.at['averageArea', 'All 4 Beds'])
            FourBedroomEffectiveRentUnit_val = screen_nulls(prop_details_df.at['effectiveRentPerUnit', 'All 4 Beds'])
            NumberOf4BedroomUnits_val = screen_nulls(prop_details_df.at['unitMixBeds', 'All 4 Beds'])
            FourBedroomConcessionsPercentage_val = screen_nulls(prop_details_df.at['concessions', 'All 4 Beds'])
        else:
            FourBedroomAskingRentUnit_val = np.nan
            FourBedroomAvgSF_val = np.nan
            FourBedroomEffectiveRentUnit_val = np.nan
            NumberOf4BedroomUnits_val = np.nan
            FourBedroomConcessionsPercentage_val = np.nan
        unit_type_metrics_package = (FourBedroomAskingRentUnit_val,
                                     FourBedroomAvgSF_val,
                                     FourBedroomEffectiveRentUnit_val,
                                     NumberOf4BedroomUnits_val,
                                     FourBedroomConcessionsPercentage_val)
    return unit_type_metrics_package


def assemble_single_property_df(file, json_response, nested_metrics_pack, additional_metrics_dict):
    """
    Create a Pandas DataFrame containing all targeted measured collected about a single property from CoStar.

    Parameters
    ----------
    file : str
        Name of the file containing the saved json_response.
    json_response : JSON object
        JSON object containing the deserialized values returned by CoStar.
    nested_metrics_pack : tuple
        A tuple of tuples containing apartment metrics by number of bedrooms.
    additional_metrics_dict : dict
        A dictionary containing all remaining property metric values.

    Returns
    -------
    temp_df : Pandas DataFrame.
        A DataFrame containng all collected metrics regarding a single property.

    """
    studio_pack, one_pack, two_pack, three_pack, four_pack = nested_metrics_pack
    temp_df = pd.DataFrame({'CoStarPropertyID': file.split('\\')[1].split('_')[0],
                            'PropertyName': json_response[6]['data']['propertyDetail']['property_info']['address']['buildingName'],
                            'PropertyAddress': json_response[6]['data']['propertyDetail']['property_info']['address']['deliveryAddress'],
                            'OneBedroomAskingRentUnit': one_pack[0],
                            'TwoBedroomAskingRentUnit': two_pack[0],
                            'ThreeBedroomAskingRentUnit': three_pack[0],
                            'FourBedroomAskingRentUnit': four_pack[0],
                            'StudioAskingRentUnit': studio_pack[0],
                            'OneBedroomAvgSF': one_pack[1],
                            'TwoBedroomAvgSF': two_pack[1],
                            'ThreeBedroomAvgSF': three_pack[1],
                            'FourBedroomAvgSF': four_pack[1],
                            'StudioAvgSF': studio_pack[1],
                            'OneBedroomEffectiveRentUnit': one_pack[2],
                            'TwoBedroomEffectiveRentUnit': two_pack[2],
                            'ThreeBedroomEffectiveRentUnit': three_pack[2],
                            'FourBedroomEffectiveRentUnit': four_pack[2],
                            'StudioEffectiveRentUnit': studio_pack[2],
                            'NumberOf1BedroomsUnits': one_pack[3],
                            'NumberOf2BedroomsUnits': two_pack[3],
                            'NumberOf3BedroomsUnits': three_pack[3],
                            'NumberOf4BedroomsUnits': four_pack[3],
                            'NumberOfStudioUnits': studio_pack[3],
                            'NumberOfUnits': additional_metrics_dict.get('unit_count'),
                            'OneBedroomConcessionsPercentage': one_pack[4],
                            'TwoBedroomConcessionsPercentage': two_pack[4],
                            'ThreeBedroomConcessionsPercentage': three_pack[4],
                            'FourBedroomConcessionsPercentage': four_pack[4],
                            'StudioConcessionsPercentage': studio_pack[4],
                            'Latitude': json_response[6]['data']['propertyDetail']['property_info']['latitude'],
                            'Longitude': json_response[6]['data']['propertyDetail']['property_info']['longitude'],
                            'PropertyManagerName': additional_metrics_dict.get('property_manager'),
                            'TrueOwnerName': additional_metrics_dict.get('true_owner'),
                            'BuildingClass': json_response[6]['data']['propertyDetail']['property_info']['bldgClass'],
                            'StarRating': json_response[6]['data']['propertyDetail']['property_info']['buildingRating'],
                            'Amenities': additional_metrics_dict.get('amenities'),
                            'YearBuilt': json_response[6]['data']['propertyDetail']['property_info']['yearBuilt'],
                            'ParkingSpaces': additional_metrics_dict.get('parking'),
                            'BuildingStories': json_response[6]['data']['propertyDetail']['property_info']['numOfStories'],
                            'PercentLeased': additional_metrics_dict.get('percent_leased'),
                            'City': json_response[6]['data']['propertyDetail']['property_info']['address']['city'],
                            'State': json_response[6]['data']['propertyDetail']['property_info']['address']['state'],
                            'Zip': additional_metrics_dict.get('zipcode')},
                          index=[0])
    return temp_df


def read_call_response_into_df(df, file):
    """
    Open and read call response from .txt file for a single property, pull
    data elements of interest from response into Pandas DataFrame.

    Parameters
    ----------
    df : Pandas DataFrame
        DataFrame containing target data element values for all property API responses already
        processed from the current scrape. The single property whose call is recorded in the
        .txt file specified by the file variable will be appended to this dataframe.
    file : str
        Full file path to .txt file containing CoStar API response for a single property.

    Returns
    -------
    df : Pandas DataFrame
        DataFrame containing the data for properties already parsed from .txt call response files
        onto which the values in file have been concatenated.

    """
    # read call response from .txt file
    with open(file, 'r+') as f:
        json_response = json.loads(f.read())
        f.close()

    prop_details_df = pd.DataFrame()
    for i in range(len(json_response[1]['data']['propertyDetail']['unit_mix_detail']['summaryItems'])):
        temp = pd.Series(json_response[1]['data']['propertyDetail']['unit_mix_detail']['summaryItems'][i])
        prop_details_df = pd.concat([prop_details_df, temp.T], axis=1, ignore_index=False)
    try:
        prop_details_df.columns = prop_details_df.loc['totals']
    except KeyError:
        pass

    try:
        PercentLeased_val = 100-float(prop_details_df.at['availablePercent', 'Totals'][:-1])
    except (ValueError, KeyError):
        PercentLeased_val = np.nan

    try:
        NumberOfUnits_val = prop_details_df.at['unitMixBeds', 'Totals']
    except KeyError:
        NumberOfUnits_val = np.nan

    try:
        PropertyManagerName_val = json_response[5]['data']['propertyDetail']['propertyContactDetails_info']['propertyManager'][0]['name']
    except (KeyError, IndexError):
        PropertyManagerName_val = np.nan

    try:
        TrueOwnerName_val = json_response[5]['data']['propertyDetail']['propertyContactDetails_info']['trueOwner'][0]['name']
    except (KeyError, IndexError):
        TrueOwnerName_val = np.nan

    Zip_val = json_response[6]['data']['propertyDetail']['property_info']['address']['postalCode']
    if pd.isna(Zip_val):
        Zip_val = np.nan
    elif len(Zip_val) == 9:
        Zip_val = Zip_val[:5]

    ParkingSpaces_val = json_response[6]['data']['propertyDetail']['property_info']['numOfParkingSpaces']
    if ParkingSpaces_val == 'None':
        ParkingSpaces_val = 0

    amenities_string_val = '; '.join(json_response[0]['data']['propertyDetail']['amenities_Info']['unitAmenities'] +
                                     json_response[0]['data']['propertyDetail']['amenities_Info']['amenities'] +
                                     json_response[0]['data']['propertyDetail']['amenities_Info']['roomAmenities'])
    if amenities_string_val == '':
        amenities_string_val = np.nan
    additional_metrics_dict = {'amenities': amenities_string_val,
                               'parking': ParkingSpaces_val,
                               'zipcode': Zip_val,
                               'true_owner': TrueOwnerName_val,
                               'property_manager': PropertyManagerName_val,
                               'percent_leased': PercentLeased_val,
                               'unit_count': NumberOfUnits_val}

    studio_pack = set_roomtype_metric_values('Studio', ('All Studios' in prop_details_df.columns), prop_details_df)
    one_bed_pack = set_roomtype_metric_values('One Bed', ('All 1 Beds' in prop_details_df.columns), prop_details_df)
    two_bed_pack = set_roomtype_metric_values('Two Bed', ('All 2 Beds' in prop_details_df.columns), prop_details_df)
    three_bed_pack = set_roomtype_metric_values('Three Bed', ('All 3 Beds' in prop_details_df.columns), prop_details_df)
    four_bed_pack = set_roomtype_metric_values('Four Bed', ('All 4 Beds' in prop_details_df.columns), prop_details_df)
    nested_metric_packs = (studio_pack,
                           one_bed_pack,
                           two_bed_pack,
                           three_bed_pack,
                           four_bed_pack)

    temp_df = assemble_single_property_df(file, json_response, nested_metric_packs, additional_metrics_dict)
    df = pd.concat([df, temp_df], axis=0, ignore_index=True)
    return df


def screen_nulls(data_element_val):
    """
    Check for a string element which cannot be cast to float.

    Parameters
    ----------
    data_element_val : str
        A string representation of a data element sourced from the API call response.

    Returns
    -------
    Float/string
        Returns np.nan (a float null value) if string is equal to '-',
        otherwise returns the unaltered string value.

    """
    if data_element_val == '-':
        return np.nan
    return data_element_val


def parse_responses(cookies_dict):
    """
    Collect all JSON responses logged in .txt files and collect the data into a
    Pandas DataFrame.

    Parameters
    ----------
    cookies_dict : dict
        Dictionary of the name: value of each cookie assigned to the webdriver post-login.

    Returns
    -------
    df : Pandas df
        A dataframe containing the .
    json_file_list : list of strings
        A list of all the .txt files read into the dataframe in the order they
        were read.
    """

    start = datetime.datetime.now()
    json_file_list = glob("C:/Users/RBurns/Documents/*.txt")
    df = pd.DataFrame()
    for file in json_file_list:
        try:
            df = read_call_response_into_df(df, file)
        except ValueError:
            df = reissue_call_and_read_response_into_df(df, file, cookies_dict)
    end = datetime.datetime.now()
    print(f'Completed parsing into dataframe in {end-start}.')
    df.to_csv(f'C:/Users/RBurns/Documents/{datetime.datetime.today().strftime("%m.%d.%Y")}_compiled_df.csv', index=False)
    return df, json_file_list


def collect_costar_data(username_string, password_string, print_progress=False):
    """
    Create, request, and receive XHR calls to CoStar API.

    Parameters
    ----------
    username_string : str
        String form of CoStar username for accessing the service.
    password_string : str
        String form of CoStar password for accessing the service.
    print_progress : bool, optional
        Boolean indicator of whether to print a progress bar. The default is False.

    Returns
    -------
    cookies_dict : dict
        Dictionary of the name: value of each cookie assigned to the webdriver post-login.

    """
    cookies_dict = get_costar_cookies(username_string, password_string)
    s = FuturesSession(executor=ProcessPoolExecutor(max_workers=16), session=Session())
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Accept-Encoding": "*",
        "Connection": "keep-alive",
        'origin': 'https://product.costar.com',
        'x-requested-with': 'XMLHttpRequest'
    }
    s.headers.update(headers)
    s.hooks['response'].append(print_url)
    s.hooks['response'].append(print_status)
    url = 'https://product.costar.com/graphql'

    print('Loading property IDs')
    properties_df = load_properties('C:/Users/RBurns/Documents/property_id_matching.csv')

    print('Creating requests.')
    start1 = datetime.datetime.now()
    futures = []
    if print_progress==True:
        for index, prop_id in tqdm(enumerate(properties_df.CoStarPropID), unit='Request calls'):
            payload = get_payload(prop_id)
            future = s.post(url, data=payload, headers=headers, cookies=cookies_dict)
            future.costar_prop_id = prop_id
            futures.append(future)
        end1 = datetime.datetime.now()
        start2 = datetime.datetime.now()
        print('Requests created. Retrieving calls.')
        for future in tqdm(as_completed(futures), leave=True, unit='Call responses'):
            resp = future.result()
            with open(f'C:/Users/RBurns/Documents/{future.costar_prop_id}_{datetime.datetime.today().strftime("%m.%d.%Y")}.txt', 'w+') as f:
                f.write(resp.text)
                f.close()
        end2 = datetime.datetime.now()
        print(f'Completed request creation in {end1-start1}, completed response reading in {end2-start2}.')
    else:
        for index, prop_id in enumerate(properties_df.CoStarPropID):
            payload = get_payload(prop_id)
            future = s.post(url, data=payload, headers=headers, cookies=cookies_dict)
            future.costar_prop_id = prop_id
            futures.append(future)
        end1 = datetime.datetime.now()
        start2 = datetime.datetime.now()
        print('Requests created. Retrieving calls.')
        for future in as_completed(futures):
            resp = future.result()
            with open(f'C:/Users/RBurns/Documents/{future.costar_prop_id}_{datetime.datetime.today().strftime("%m.%d.%Y")}.txt', 'w+') as f:
                f.write(resp.text)
                f.close()
        end2 = datetime.datetime.now()
        print(f'Completed request creation in {end1-start1}, completed response reading in {end2-start2}.')
    return cookies_dict


def main(print_progress=False, sql_connection_string=None, sql_table_name=None,
         username_string=None, password_string=None):
    """
    Run full program to send/receive API calls from CoStar, parse the call responses,
    save the responses to a .csv file for backup, and append the latest data from
    the scrape into the SQL OperationsReporting database.

    The values for sql_connection_string, sql_table_name, username_string, and
    password_string for privacy protections.

    Parameters
    ----------
    print_progress : bool, optional
        Boolean indicator of whether to print a progress bar. The default is False.

    Returns
    -------
    None.

    """
    start = datetime.datetime.now()
    cookies_dict = collect_costar_data(username_string, password_string, print_progress)
    total_day_df, json_file_list = parse_responses(cookies_dict)
    if sql_connection_string == None:
        sql_connection_string = input('Enter the connection string to connect to the SQL Server: ')
    if sql_table_name == None:
        sql_table_name = input('Enter the name of the target table on the SQL Server: ')
    post_to_db(sql_connection_string, sql_table_name, total_day_df)
    for file in json_file_list:
        os.remove(file)
    end = datetime.datetime.now()
    print(f'Full program run completed in {end-start}.')
    return


if __name__ == "__main__":
    main(print_progress=False)
