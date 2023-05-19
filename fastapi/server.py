import uvicorn
from fastapi import FastAPI, Query, Response, status
import pandas as pd

app = FastAPI()

data_df = pd.DataFrame()
data_centroids_df = pd.DataFrame()


@app.get("/")
def get_root():
    return {"success": True, "message": "Hello world"}


@app.get("/data")
def get_data(response: Response,
             min: bool = Query(
                 None, description='Query parameter for minified data'),
             district: str = Query(
                 None, description='Query parameter for district'),
             dow: str = Query(None, description='Query parameter for day of week')):
    try:
        global data_df
        if data_df.empty:
            response.status_code = status.HTTP_418_IM_A_TEAPOT
            return {'success': False, 'message': 'No data'}
        if district in data_df['district'].values:
            data_df = data_df[data_df['district'] == district]
        if dow in data_df['DoW'].values:
            data_df = data_df[data_df['DoW'] == dow]
        if min:
            min_df = data_df.loc[:, 'cluster']
            min_data = min_df.to_dict()
            return {"success": True, 'count': min_df.shape[0], "data": min_data}
    except:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"success": False, 'message': 'Something wrong bro'}
    data = data_df.to_dict('index')
    return {"success": True, 'count': data_df.shape[0], "data": data}


@app.get("/data-centroids")
def get_data_centroids(response: Response):
    try:
        global data_centroids_df
        if data_centroids_df.empty:
            response.status_code = status.HTTP_418_IM_A_TEAPOT
            return {'success': False, 'message': 'No data'}
    except:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"success": False, 'message': 'Something wrong bro'}
    data_centroids = data_centroids_df.to_dict('index')
    return {"success": True, 'count': data_centroids_df.shape[0], "data": data_centroids}


@app.get("/data/{ticket_id}")
def get_data_ticket(response: Response, ticket_id: str):
    try:
        global data_df
        if data_df.empty:
            response.status_code = status.HTTP_418_IM_A_TEAPOT
            return {'success': False, 'message': 'No data'}
        if ticket_id not in data_df.index:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {'success': False, 'message': 'Invalid ticket id'}
    except:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"success": False, 'message': 'Something wrong bro'}
    ticket_df = data_df.loc[ticket_id]
    ticket_data = ticket_df.to_dict()
    return {"success": True, "data": ticket_data}


@app.post("/data")
def post_data(response: Response, data: dict):
    try:
        global data_df
        data_df = pd.DataFrame.from_dict(data, orient='index')
    except:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"success": False, 'message': 'Something wrong bro'}
    return {"success": True, "count": data_df.shape[0], "message": "Data updated"}


@app.post("/data-centroids")
def post_data_centroids(response: Response, data_centroids: dict):
    try:
        global data_centroids_df
        data_centroids_df = pd.DataFrame.from_dict(
            data_centroids, orient='index')
    except:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"success": False, 'message': 'Something wrong bro'}
    return {"success": True, "count": data_centroids_df.shape[0], "message": "Centroids updated"}


if __name__ == "__main__":
    uvicorn.run("server:app", port=6969, reload=True, log_level="info")
