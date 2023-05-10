import logo from "../logo.svg";

export function Logo(){
    return (
        <div style={{
            display: 'flex',
            height: '100%',
            alignItems: 'center',
            padding: '10px 0'
        }}>
            <img src={logo} alt='logo' style={{
                height: '100%'
            }}/>
            <div style={{
                wordBreak: 'keep-all',
                fontSize: '25px',
                lineHeight: 'normal',
                marginLeft: '5px',
                fontWeight: 'bold'
            }}>Kvrocks Controller</div>
        </div>
    )
}