export async function sleep(timeMilliSecond: number) {
    return new Promise(res => setTimeout(res, timeMilliSecond));
}