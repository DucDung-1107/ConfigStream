"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  IconInnerShadowTop,
  IconDashboard,
  IconGitBranch,
  IconShieldCheck,
  IconActivity,
  IconWorld,
  IconChartBar,
  IconServer,
} from "@tabler/icons-react";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  useSidebar,
} from "@/components/ui/sidebar";

export const routes = [
  { url: "/", title: "Overview", icon: IconDashboard },
  { url: "/history/", title: "Infrastructure History", icon: IconGitBranch },
  { url: "/rpki/", title: "RPKI States", icon: IconShieldCheck },
  { url: "/ris/", title: "RIS Events", icon: IconActivity },
  { url: "/dns/", title: "DNS Measurements", icon: IconWorld },
  { url: "/experiments/", title: "Experiments", icon: IconChartBar },
  { url: "/operations/", title: "Pipeline & Operations", icon: IconServer },
];
export function AppSidebar(props: React.ComponentProps<typeof Sidebar>) {
  const path = usePathname();
  const { setOpenMobile } = useSidebar();
  return (
    <Sidebar collapsible="offcanvas" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              asChild
              className="data-[slot=sidebar-menu-button]:p-1.5!"
            >
              <Link href="/" onClick={() => setOpenMobile(false)}>
                <IconInnerShadowTop className="size-5!" />
                <span className="text-base font-semibold">ConfigStream</span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Research workspace</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {routes.map((route) => (
                <SidebarMenuItem key={route.url}>
                  <SidebarMenuButton
                    asChild
                    isActive={
                      path === route.url || path === route.url.slice(0, -1)
                    }
                    tooltip={route.title}
                  >
                    <Link href={route.url} onClick={() => setOpenMobile(false)}>
                      <route.icon />
                      <span>{route.title}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter>
        <div className="px-2 py-3 text-xs leading-relaxed text-muted-foreground">
          Selected evidence scopes
          <br />
          IT4043E · local Kubernetes
          <br />
          Read-only research demonstration
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
