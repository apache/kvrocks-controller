import { Layout } from 'antd';
import { Logo } from './components/Logo';
import './App.css';
import { Sidebar } from './components/sidebar/Sidebar';

function App() {
    return (
        <Layout>
            <Layout.Header style={{backgroundColor: 'white'}}>
                <Logo></Logo>
            </Layout.Header>
            <Layout style={{minHeight: 'calc(100vh - 64px)'}}>
                <Layout.Sider
                    theme='light'
                    collapsible
                >
                    <Sidebar/>
                </Layout.Sider>
            </Layout>
        </Layout>
    );
}

export default App;
