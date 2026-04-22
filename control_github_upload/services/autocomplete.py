def find_first_user_id_for_sector(users, sector_id):
    sector_id = str(sector_id or "").strip()
    if not sector_id:
        return None
    for user in users:
        if str(user.get("sector_id") or "").strip() == sector_id:
            return user.get("id")
    return None
