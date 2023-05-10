import { Menu, MenuProps } from 'antd';
import { useEffect, useState } from 'react';
import { useApi } from '../../hooks/useApi';

type MenuItem = Required<MenuProps>['items'][number];

export function Sidebar() {
    const [menuItems, setMenuItems] = useState<MenuItem[]>([
        {
            key: 'aaa',
            label: 'aaa'
        },{
            key: 'bbb',
            label: 'bbb'
        },{
            key: 'ccc',
            label: 'ccc',
            children: [
                {
                    key: 'xxx',
                    label: 'xxx',
                }
            ]
        },
    ]);
    useEffect(() => {
        setTimeout(() => {
            setMenuItems(old => {
                const m = old[1];
                (m as any).children = [
                    {
                        key: 'zzzz',
                        label: 'zzzz',
                    }
                ];
                return [...old];
            });
        }, 2000);
    },[]);
    useApi('listNamespace', undefined);
    return <Menu
        theme='light'
        mode='inline'
        items={menuItems}
    ></Menu>;
}