from trophy_logic import calculate_trophy_change

def test_trophy_logic():
    print("--- Testing Trophy Logic ---")
    
    # Solo Showdown
    v1 = calculate_trophy_change("soloShowdown", 250, rank=1)
    print(f"Solo Showdown (250 trophies, 1st): Expected +13, Got {v1}")
    assert v1 == 13
    
    v2 = calculate_trophy_change("soloShowdown", 1050, rank=10)
    print(f"Solo Showdown (1050 trophies, 10th): Expected -6, Got {v2}")
    assert v2 == -6

    # Team Modes
    v3 = calculate_trophy_change("brawlBall", 650, result="victory")
    print(f"Brawl Ball (650 trophies, Victory): Expected +10, Got {v3}")
    assert v3 == 10
    
    v4 = calculate_trophy_change("brawlBall", 650, result="defeat")
    print(f"Brawl Ball (650 trophies, Defeat): Expected -3, Got {v4}")
    assert v4 == -3

    # Duels
    v5 = calculate_trophy_change("duels", 900, result="victory")
    print(f"Duels (900 trophies, Victory): Expected +12, Got {v5}")
    assert v5 == 12
    
    v6 = calculate_trophy_change("duels", 1300, result="defeat")
    print(f"Duels (1300 trophies, Defeat): Expected -9, Got {v6}")
    assert v6 == -9

    print("\n--- All tests passed! ---")

if __name__ == "__main__":
    test_trophy_logic()
